import {
  Inject,
  Injectable,
  Logger,
  NotFoundException,
  OnModuleInit,
} from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { randomUUID } from 'crypto';
import { firstValueFrom } from 'rxjs';
import {
  ACTIVATION_STATUS,
  ActivationCompletedPayload,
  ActivationFailedPayload,
  ActivationRequestedPayload,
  BillingEventPayload,
  createEventEnvelope,
  EVENT_TYPES,
  EventEnvelope,
  KAFKA_TOPICS,
  parseEventEnvelope,
  ProvisioningEventPayload,
  SERVICE_SOURCES,
  SIMULATE_FAILURE_MODES,
  STEP_STATUS,
  toKafkaMessage,
} from '@app/contracts';
import {
  Activation,
  ActivationDocument,
} from './schemas/activation.schema';
import { CreateActivationDto } from './dto/create-activation.dto';
import { ActivationsGateway } from './activations.gateway';

@Injectable()
export class ActivationsService implements OnModuleInit {
  private readonly logger = new Logger(ActivationsService.name);

  constructor(
    @InjectModel(Activation.name)
    private readonly activationModel: Model<ActivationDocument>,
    @Inject('KAFKA_PRODUCER')
    private readonly kafkaClient: ClientKafka,
    private readonly gateway: ActivationsGateway,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
    this.logger.log('✅ Productor Kafka conectado en activation-api');
  }

  /**
   * RF-01, RF-02, RF-03:
   * Crea la activación en estado PENDING, publica ActivationRequested con key=customerId
   * y retorna de inmediato para responder HTTP 202 Accepted.
   */
  async createActivation(dto: CreateActivationDto) {
    const activationId = `act-${randomUUID().slice(0, 8)}`;
    const now = new Date().toISOString();
    const channel = dto.channel ?? 'web';
    const simulateFailure =
      dto.simulateFailure ?? SIMULATE_FAILURE_MODES.NONE;

    const envelope = createEventEnvelope<ActivationRequestedPayload>({
      eventType: EVENT_TYPES.ACTIVATION_REQUESTED,
      correlationId: activationId,
      customerId: dto.customerId,
      source: SERVICE_SOURCES.ACTIVATION_API,
      simulateFailure,
      occurredAt: now,
      payload: {
        planId: dto.planId,
        channel,
        simulateFailure,
      },
    });

    const activation = await this.activationModel.create({
      _id: activationId,
      customerId: dto.customerId,
      planId: dto.planId,
      channel,
      status: ACTIVATION_STATUS.PENDING,
      simulateFailure,
      steps: {
        billing: { status: STEP_STATUS.PENDING, at: null },
        provisioning: { status: STEP_STATUS.PENDING, at: null },
      },
      history: [
        {
          eventId: envelope.eventId,
          eventType: envelope.eventType,
          at: envelope.occurredAt,
          source: envelope.source,
          payload: envelope.payload,
        },
      ],
    });

    await firstValueFrom(
      this.kafkaClient.emit(
        KAFKA_TOPICS.ACTIVATION_REQUESTED,
        toKafkaMessage(envelope),
      ),
    );

    this.logger.log(
      `🚀 [${activationId}] Activación creada en PENDING y publicado ${EVENT_TYPES.ACTIVATION_REQUESTED} (customerId=${dto.customerId}, simulateFailure=${simulateFailure})`,
    );

    this.gateway.emitActivationUpdate(activation.toObject(), envelope);

    return {
      activationId: activation._id,
      status: activation.status,
      customerId: activation.customerId,
      planId: activation.planId,
      channel: activation.channel,
      simulateFailure: activation.simulateFailure,
      createdAt: now,
    };
  }

  /**
   * RF-09: Devuelve el estado actual y el historial completo de eventos de una activación.
   */
  async getActivationById(id: string): Promise<Record<string, any>> {
    const activation = await this.activationModel.findById(id).lean();
    if (!activation) {
      throw new NotFoundException(`Activación ${id} no encontrada`);
    }
    return {
      activationId: activation._id,
      ...activation,
    };
  }

  /**
   * Lista las últimas activaciones (útil para poblar inicialmente la Demo UI).
   */
  async listActivations(): Promise<Array<Record<string, any>>> {
    const list = await this.activationModel
      .find()
      .sort({ createdAt: -1 })
      .limit(50)
      .lean();
    return list.map((item) => ({
      activationId: item._id,
      ...item,
    }));
  }

  /**
   * Procesa los eventos emitidos por billing-service en el tópico billing.events.
   */
  async handleBillingEvent(raw: any): Promise<void> {
    const envelope = parseEventEnvelope<BillingEventPayload>(raw);
    const { correlationId, eventType, occurredAt, eventId, source, payload } =
      envelope;

    this.logger.log(
      `📥 [${correlationId}] Recibido de billing.events -> ${eventType}`,
    );

    if (eventType === EVENT_TYPES.BILLING_ACCOUNT_CREATED) {
      const updated = await this.activationModel
        .findOneAndUpdate(
          { _id: correlationId },
          {
            $set: {
              'steps.billing': { status: STEP_STATUS.OK, at: occurredAt },
            },
            $push: {
              history: {
                eventId,
                eventType,
                at: occurredAt,
                source,
                payload,
              },
            },
          },
          { new: true },
        )
        .lean();

      if (!updated) return;
      await this.evaluateSagaProgress(updated, envelope);
      return;
    }

    if (eventType === EVENT_TYPES.BILLING_FAILED) {
      const reason = payload?.reason ?? 'Fallo en creación de cuenta de facturación';
      const updated = await this.activationModel
        .findOneAndUpdate(
          { _id: correlationId },
          {
            $set: {
              'steps.billing': {
                status: STEP_STATUS.FAILED,
                at: occurredAt,
                reason,
              },
            },
            $push: {
              history: {
                eventId,
                eventType,
                at: occurredAt,
                source,
                payload,
              },
            },
          },
          { new: true },
        )
        .lean();

      if (!updated) return;
      await this.triggerSagaFailure(updated, envelope, 'billing', reason);
      return;
    }

    if (eventType === EVENT_TYPES.BILLING_ACCOUNT_CANCELLED) {
      // Evento de compensación posterior al fallo de la Saga
      const updated = await this.activationModel
        .findOneAndUpdate(
          { _id: correlationId },
          {
            $set: {
              'steps.billing': {
                status: STEP_STATUS.CANCELLED,
                at: occurredAt,
                reason: payload?.reason ?? 'Cuenta anulada por compensación Saga',
              },
            },
            $push: {
              history: {
                eventId,
                eventType,
                at: occurredAt,
                source,
                payload,
              },
            },
          },
          { new: true },
        )
        .lean();

      if (updated) {
        this.logger.warn(
          `🔄 [${correlationId}] Compensación registrada en historial: ${EVENT_TYPES.BILLING_ACCOUNT_CANCELLED}`,
        );
        this.gateway.emitActivationUpdate(updated, envelope);
      }
    }
  }

  /**
   * Procesa los eventos emitidos por provisioning-service en el tópico provisioning.events.
   */
  async handleProvisioningEvent(raw: any): Promise<void> {
    const envelope = parseEventEnvelope<ProvisioningEventPayload>(raw);
    const { correlationId, eventType, occurredAt, eventId, source, payload } =
      envelope;

    this.logger.log(
      `📥 [${correlationId}] Recibido de provisioning.events -> ${eventType}`,
    );

    if (eventType === EVENT_TYPES.PROVISIONING_COMPLETED) {
      const updated = await this.activationModel
        .findOneAndUpdate(
          { _id: correlationId },
          {
            $set: {
              'steps.provisioning': { status: STEP_STATUS.OK, at: occurredAt },
            },
            $push: {
              history: {
                eventId,
                eventType,
                at: occurredAt,
                source,
                payload,
              },
            },
          },
          { new: true },
        )
        .lean();

      if (!updated) return;
      await this.evaluateSagaProgress(updated, envelope);
      return;
    }

    if (eventType === EVENT_TYPES.PROVISIONING_FAILED) {
      const reason =
        payload?.reason ?? 'Fallo técnico en aprovisionamiento de línea';
      const updated = await this.activationModel
        .findOneAndUpdate(
          { _id: correlationId },
          {
            $set: {
              'steps.provisioning': {
                status: STEP_STATUS.FAILED,
                at: occurredAt,
                reason,
              },
            },
            $push: {
              history: {
                eventId,
                eventType,
                at: occurredAt,
                source,
                payload,
              },
            },
          },
          { new: true },
        )
        .lean();

      if (!updated) return;
      await this.triggerSagaFailure(updated, envelope, 'provisioning', reason);
    }
  }

  /**
   * Evalúa atómicamente si la Saga debe pasar a IN_PROGRESS o completarse en ACTIVE (RF-05).
   * Si la activación ya había fallado antes, ignora el éxito tardío salvo dejarlo en history (RF-06).
   */
  private async evaluateSagaProgress(
    current: Activation,
    stepEnvelope: EventEnvelope<any>,
  ): Promise<void> {
    const activationId = current._id;

    // Si ya está en estado terminal (FAILED o ACTIVE), solo notificamos el historial por WS
    if (
      current.status === ACTIVATION_STATUS.FAILED ||
      current.status === ACTIVATION_STATUS.ACTIVE
    ) {
      this.gateway.emitActivationUpdate(current, stepEnvelope);
      return;
    }

    const bothOk =
      current.steps?.billing?.status === STEP_STATUS.OK &&
      current.steps?.provisioning?.status === STEP_STATUS.OK;

    if (bothOk) {
      const completedAt = new Date().toISOString();
      const completedEnvelope =
        createEventEnvelope<ActivationCompletedPayload>({
          eventType: EVENT_TYPES.ACTIVATION_COMPLETED,
          correlationId: activationId,
          customerId: current.customerId,
          source: SERVICE_SOURCES.ACTIVATION_API,
          occurredAt: completedAt,
          payload: {
            planId: current.planId,
            completedAt,
          },
        });

      // Transición atómica a ACTIVE asegurando que solo se emita ActivationCompleted una vez
      const finalized = await this.activationModel
        .findOneAndUpdate(
          {
            _id: activationId,
            status: {
              $nin: [ACTIVATION_STATUS.ACTIVE, ACTIVATION_STATUS.FAILED],
            },
            'steps.billing.status': STEP_STATUS.OK,
            'steps.provisioning.status': STEP_STATUS.OK,
          },
          {
            $set: { status: ACTIVATION_STATUS.ACTIVE },
            $push: {
              history: {
                eventId: completedEnvelope.eventId,
                eventType: completedEnvelope.eventType,
                at: completedEnvelope.occurredAt,
                source: completedEnvelope.source,
                payload: completedEnvelope.payload,
              },
            },
          },
          { new: true },
        )
        .lean();

      if (finalized) {
        await firstValueFrom(
          this.kafkaClient.emit(
            KAFKA_TOPICS.ACTIVATION_EVENTS,
            toKafkaMessage(completedEnvelope),
          ),
        );

        this.logger.log(
          `🎉 [${activationId}] Saga completada -> Estado ACTIVE. Publicado ${EVENT_TYPES.ACTIVATION_COMPLETED}`,
        );

        this.gateway.emitActivationUpdate(current, stepEnvelope);
        this.gateway.emitActivationUpdate(finalized, completedEnvelope);
        return;
      }
    }

    // Primer paso exitoso recibido -> pasar de PENDING a IN_PROGRESS
    const inProgressDoc = await this.activationModel
      .findOneAndUpdate(
        { _id: activationId, status: ACTIVATION_STATUS.PENDING },
        { $set: { status: ACTIVATION_STATUS.IN_PROGRESS } },
        { new: true },
      )
      .lean();

    const docToEmit = inProgressDoc ?? current;
    this.logger.log(
      `⏳ [${activationId}] Primer paso completado -> Estado ${docToEmit.status}`,
    );
    this.gateway.emitActivationUpdate(docToEmit, stepEnvelope);
  }

  /**
   * RF-06: Ante el primer fallo de billing o provisioning, pasa la activación a FAILED
   * y publica inmediatamente ActivationFailed en activation.events.
   */
  private async triggerSagaFailure(
    current: Activation,
    stepEnvelope: EventEnvelope<any>,
    failedStep: 'billing' | 'provisioning',
    reason: string,
  ): Promise<void> {
    const activationId = current._id;
    const failedAt = new Date().toISOString();

    const failedEnvelope = createEventEnvelope<ActivationFailedPayload>({
      eventType: EVENT_TYPES.ACTIVATION_FAILED,
      correlationId: activationId,
      customerId: current.customerId,
      source: SERVICE_SOURCES.ACTIVATION_API,
      occurredAt: failedAt,
      payload: {
        planId: current.planId,
        failedStep,
        reason,
        failedAt,
      },
    });

    const failedDoc = await this.activationModel
      .findOneAndUpdate(
        {
          _id: activationId,
          status: {
            $nin: [ACTIVATION_STATUS.FAILED, ACTIVATION_STATUS.ACTIVE],
          },
        },
        {
          $set: { status: ACTIVATION_STATUS.FAILED },
          $push: {
            history: {
              eventId: failedEnvelope.eventId,
              eventType: failedEnvelope.eventType,
              at: failedEnvelope.occurredAt,
              source: failedEnvelope.source,
              payload: failedEnvelope.payload,
            },
          },
        },
        { new: true },
      )
      .lean();

    if (failedDoc) {
      await firstValueFrom(
        this.kafkaClient.emit(
          KAFKA_TOPICS.ACTIVATION_EVENTS,
          toKafkaMessage(failedEnvelope),
        ),
      );

      this.logger.error(
        `❌ [${activationId}] Saga falló en ${failedStep} (${reason}) -> Estado FAILED. Publicado ${EVENT_TYPES.ACTIVATION_FAILED}`,
      );

      this.gateway.emitActivationUpdate(current, stepEnvelope);
      this.gateway.emitActivationUpdate(failedDoc, failedEnvelope);
    } else {
      // Ya estaba en FAILED, solo emitimos el evento al timeline WS
      this.gateway.emitActivationUpdate(current, stepEnvelope);
    }
  }
}
