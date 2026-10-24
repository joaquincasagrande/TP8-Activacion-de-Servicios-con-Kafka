import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { parseEventEnvelope } from '@app/contracts';
import { EventLog, EventLogDocument } from './schemas/event-log.schema';

@Injectable()
export class CrmService {
  private readonly logger = new Logger(CrmService.name);

  constructor(
    @InjectModel(EventLog.name)
    private readonly eventLogModel: Model<EventLogDocument>,
  ) {}

  /**
   * RF-10: Consumidor pasivo de auditoría que registra todos los eventos del sistema
   * de forma idempotente en la colección event_log.
   */
  async recordEvent(topic: string, raw: any): Promise<void> {
    const envelope = parseEventEnvelope<Record<string, any>>(raw);
    const {
      eventId,
      correlationId,
      customerId,
      eventType,
      version,
      source,
      occurredAt,
    } = envelope;

    await this.eventLogModel.updateOne(
      { eventId },
      {
        $setOnInsert: {
          eventId,
          correlationId,
          customerId,
          topic,
          eventType,
          version: version ?? 1,
          source: String(source),
          occurredAt,
          envelope: envelope as Record<string, any>,
        },
      },
      { upsert: true },
    );

    this.logger.log(
      `📊 [CRM Audit] [${correlationId}] Registrado ${eventType} desde ${topic} (eventId=${eventId})`,
    );
  }
}
