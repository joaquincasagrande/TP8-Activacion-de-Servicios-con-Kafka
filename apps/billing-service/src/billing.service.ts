import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { firstValueFrom } from 'rxjs';
import {
  ActivationFailedPayload,
  ActivationRequestedPayload,
  BillingEventPayload,
  createEventEnvelope,
  EVENT_TYPES,
  KAFKA_TOPICS,
  parseEventEnvelope,
  SERVICE_SOURCES,
  SIMULATE_FAILURE_MODES,
  toKafkaMessage,
} from '@app/contracts';
import {
  BillingAccount,
  BillingAccountDocument,
} from './schemas/billing-account.schema';

@Injectable()
export class BillingService implements OnModuleInit {
  private readonly logger = new Logger(BillingService.name);

  constructor(
    @InjectModel(BillingAccount.name)
    private readonly billingModel: Model<BillingAccountDocument>,
    @Inject('KAFKA_PRODUCER')
    private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
    this.logger.log('✅ Productor Kafka conectado en billing-service');
  }

  /**
   * RF-04: Procesa ActivationRequested en el grupo billing-svc.
   * Crea la cuenta de facturación (o falla si simulateFailure === 'billing')
   * y publica el resultado en billing.events.
   */
  async handleActivationRequested(raw: any): Promise<void> {
    const envelope = parseEventEnvelope<ActivationRequestedPayload>(raw);
    const activationId = envelope.correlationId;
    const customerId = envelope.customerId;
    const planId = envelope.payload?.planId ?? 'PLAN-DEFAULT';
    const simulateFailure =
      envelope.simulateFailure ??
      envelope.payload?.simulateFailure ??
      SIMULATE_FAILURE_MODES.NONE;
    const accountId = `bill-${activationId}`;

    this.logger.log(
      `💳 [${activationId}] Procesando ActivationRequested para cliente ${customerId} (simulateFailure=${simulateFailure})`,
    );

    // Simulación de fallo forzado en facturación (Fase 2)
    if (simulateFailure === SIMULATE_FAILURE_MODES.BILLING) {
      const reason = 'Fallo simulado en creación de cuenta de facturación';
      await this.billingModel.findOneAndUpdate(
        { activationId },
        {
          $set: {
            accountId,
            customerId,
            planId,
            status: 'FAILED',
            reason,
          },
        },
        { upsert: true, new: true },
      );

      const failedEvent = createEventEnvelope<BillingEventPayload>({
        eventType: EVENT_TYPES.BILLING_FAILED,
        correlationId: activationId,
        customerId,
        source: SERVICE_SOURCES.BILLING_SERVICE,
        simulateFailure,
        payload: {
          accountId,
          planId,
          status: 'FAILED',
          reason,
          simulateFailure,
        },
      });

      await firstValueFrom(
        this.kafkaClient.emit(
          KAFKA_TOPICS.BILLING_EVENTS,
          toKafkaMessage(failedEvent),
        ),
      );

      this.logger.warn(
        `❌ [${activationId}] Publicado ${EVENT_TYPES.BILLING_FAILED} en ${KAFKA_TOPICS.BILLING_EVENTS}`,
      );
      return;
    }

    // Verificar caso borde: si ActivationFailed llegó antes que ActivationRequested
    const existing = await this.billingModel.findOne({ activationId }).lean();
    if (existing && existing.status === 'CANCELLED') {
      this.logger.warn(
        `⚠️ [${activationId}] Caso borde detectado: la activación ya fue marcada como CANCELLED antes de crear la cuenta. Emitiendo BillingAccountCancelled.`,
      );
      await this.publishBillingCancelled(
        activationId,
        customerId,
        accountId,
        planId,
        'Cuenta anulada preventivamente por fallo previo de la activación',
      );
      return;
    }

    // Camino normal: crear cuenta con estado CREATED
    await this.billingModel.findOneAndUpdate(
      { activationId },
      {
        $setOnInsert: {
          activationId,
          accountId,
          customerId,
          planId,
          status: 'CREATED',
        },
      },
      { upsert: true, new: true },
    );

    const createdEvent = createEventEnvelope<BillingEventPayload>({
      eventType: EVENT_TYPES.BILLING_ACCOUNT_CREATED,
      correlationId: activationId,
      customerId,
      source: SERVICE_SOURCES.BILLING_SERVICE,
      simulateFailure,
      payload: {
        accountId,
        planId,
        status: 'CREATED',
        simulateFailure,
      },
    });

    await firstValueFrom(
      this.kafkaClient.emit(
        KAFKA_TOPICS.BILLING_EVENTS,
        toKafkaMessage(createdEvent),
      ),
    );

    this.logger.log(
      `✅ [${activationId}] Cuenta ${accountId} CREATED -> Publicado ${EVENT_TYPES.BILLING_ACCOUNT_CREATED}`,
    );
  }

  /**
   * RF-06: Compensación del patrón Saga.
   * Escucha activation.events; si recibe ActivationFailed y la cuenta había sido creada,
   * la pasa a CANCELLED y publica BillingAccountCancelled en billing.events.
   */
  async handleActivationEvent(raw: any): Promise<void> {
    const envelope = parseEventEnvelope<ActivationFailedPayload>(raw);

    if (envelope.eventType !== EVENT_TYPES.ACTIVATION_FAILED) {
      return;
    }

    const activationId = envelope.correlationId;
    const customerId = envelope.customerId;
    const planId = envelope.payload?.planId ?? 'PLAN-DEFAULT';
    const failedStep = envelope.payload?.failedStep;

    // Si el fallo original fue del propio billing-service, no hay cuenta CREATED que compensar
    if (failedStep === 'billing') {
      return;
    }

    const existing = await this.billingModel.findOne({ activationId }).lean();
    const accountId = existing?.accountId ?? `bill-${activationId}`;
    const compensationReason = `Compensación Saga: activación falló en ${failedStep ?? 'otro servicio'} (${envelope.payload?.reason ?? 'sin detalle'})`;

    if (existing && existing.status === 'CREATED') {
      await this.billingModel.updateOne(
        { activationId },
        {
          $set: {
            status: 'CANCELLED',
            reason: compensationReason,
          },
        },
      );

      await this.publishBillingCancelled(
        activationId,
        customerId,
        accountId,
        existing.planId ?? planId,
        compensationReason,
      );
      return;
    }

    if (!existing) {
      // Caso borde: ActivationFailed llegó antes que ActivationRequested termine en billing
      await this.billingModel.updateOne(
        { activationId },
        {
          $setOnInsert: {
            activationId,
            accountId,
            customerId,
            planId,
            status: 'CANCELLED',
            reason: compensationReason,
          },
        },
        { upsert: true },
      );

      this.logger.warn(
        `🛡️ [${activationId}] Registrada marca de compensación temprana (CANCELLED) ante llegada anticipada de ActivationFailed`,
      );
    }
  }

  private async publishBillingCancelled(
    activationId: string,
    customerId: string,
    accountId: string,
    planId: string,
    reason: string,
  ): Promise<void> {
    const cancelledEvent = createEventEnvelope<BillingEventPayload>({
      eventType: EVENT_TYPES.BILLING_ACCOUNT_CANCELLED,
      correlationId: activationId,
      customerId,
      source: SERVICE_SOURCES.BILLING_SERVICE,
      payload: {
        accountId,
        planId,
        status: 'CANCELLED',
        reason,
      },
    });

    await firstValueFrom(
      this.kafkaClient.emit(
        KAFKA_TOPICS.BILLING_EVENTS,
        toKafkaMessage(cancelledEvent),
      ),
    );

    this.logger.warn(
      `🔄 [${activationId}] Compensación ejecutada: cuenta ${accountId} CANCELLED -> Publicado ${EVENT_TYPES.BILLING_ACCOUNT_CANCELLED}`,
    );
  }
}
