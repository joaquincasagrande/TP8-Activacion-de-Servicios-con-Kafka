import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { firstValueFrom } from 'rxjs';
import {
  ActivationRequestedPayload,
  createEventEnvelope,
  EVENT_TYPES,
  KAFKA_TOPICS,
  parseEventEnvelope,
  ProvisioningEventPayload,
  SERVICE_SOURCES,
  SIMULATE_FAILURE_MODES,
  toKafkaMessage,
} from '@app/contracts';
import {
  ProvisioningOrder,
  ProvisioningOrderDocument,
} from './schemas/provisioning-order.schema';

@Injectable()
export class ProvisioningService implements OnModuleInit {
  private readonly logger = new Logger(ProvisioningService.name);

  constructor(
    @InjectModel(ProvisioningOrder.name)
    private readonly provisioningModel: Model<ProvisioningOrderDocument>,
    @Inject('KAFKA_PRODUCER')
    private readonly kafkaClient: ClientKafka,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.kafkaClient.connect();
    this.logger.log('✅ Productor Kafka conectado en provisioning-service');
  }

  /**
   * RF-04: Procesa ActivationRequested en paralelo a billing-service (grupo provisioning-svc).
   * Aplica un retardo simulado de 1 a 3 segundos y publica ProvisioningCompleted o ProvisioningFailed.
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
    const orderId = `prov-${activationId}`;

    // Retardo simulado de 1 a 3 segundos (1000ms a 3000ms)
    const delayMs = Math.floor(Math.random() * 2000) + 1000;
    this.logger.log(
      `⚙️ [${activationId}] Iniciando aprovisionamiento técnico para ${customerId} (delay=${delayMs}ms, simulateFailure=${simulateFailure})...`,
    );

    await new Promise((resolve) => setTimeout(resolve, delayMs));

    if (simulateFailure === SIMULATE_FAILURE_MODES.PROVISIONING) {
      const reason =
        'Fallo técnico simulado en aprovisionamiento de red/línea';

      await this.provisioningModel.findOneAndUpdate(
        { activationId },
        {
          $set: {
            orderId,
            customerId,
            planId,
            status: 'FAILED',
            durationMs: delayMs,
            reason,
          },
        },
        { upsert: true, new: true },
      );

      const failedEvent = createEventEnvelope<ProvisioningEventPayload>({
        eventType: EVENT_TYPES.PROVISIONING_FAILED,
        correlationId: activationId,
        customerId,
        source: SERVICE_SOURCES.PROVISIONING_SERVICE,
        simulateFailure,
        payload: {
          orderId,
          planId,
          status: 'FAILED',
          durationMs: delayMs,
          reason,
          simulateFailure,
        },
      });

      await firstValueFrom(
        this.kafkaClient.emit(
          KAFKA_TOPICS.PROVISIONING_EVENTS,
          toKafkaMessage(failedEvent),
        ),
      );

      this.logger.warn(
        `❌ [${activationId}] Aprovisionamiento falló tras ${delayMs}ms -> Publicado ${EVENT_TYPES.PROVISIONING_FAILED}`,
      );
      return;
    }

    await this.provisioningModel.findOneAndUpdate(
      { activationId },
      {
        $set: {
          orderId,
          customerId,
          planId,
          status: 'COMPLETED',
          durationMs: delayMs,
        },
      },
      { upsert: true, new: true },
    );

    const completedEvent = createEventEnvelope<ProvisioningEventPayload>({
      eventType: EVENT_TYPES.PROVISIONING_COMPLETED,
      correlationId: activationId,
      customerId,
      source: SERVICE_SOURCES.PROVISIONING_SERVICE,
      simulateFailure,
      payload: {
        orderId,
        planId,
        status: 'COMPLETED',
        durationMs: delayMs,
        simulateFailure,
      },
    });

    await firstValueFrom(
      this.kafkaClient.emit(
        KAFKA_TOPICS.PROVISIONING_EVENTS,
        toKafkaMessage(completedEvent),
      ),
    );

    this.logger.log(
      `✅ [${activationId}] Orden ${orderId} COMPLETED en ${delayMs}ms -> Publicado ${EVENT_TYPES.PROVISIONING_COMPLETED}`,
    );
  }
}
