import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import { EventLog } from './schemas';

@Controller()
export class AppController {
  constructor(
    @InjectModel(EventLog.name) private eventLogModel: Model<EventLog>,
  ) { }

  private async persistEvent(message: any, topicName: string) {
    const event = message?.value || message;
    if (!event?.eventId) return;

    try {
      await this.eventLogModel.create({
        eventId: event.eventId,
        eventType: event.eventType,
        correlationId: event.correlationId,
        customerId: event.customerId,
        source: event.source,
        payload: event.payload,
        occurredAt: event.occurredAt || new Date().toISOString(),
      });
      console.log(`[CRM Log] Evento guardado desde '${topicName}': ${event.eventType} (${event.correlationId})`);
    } catch (err: any) {
      if (err.code === 11000) {
        console.log(`[CRM Log] Evento duplicado ignorado: ${event.eventId}`);
        return;
      }
      console.error(`[CRM Log] Error guardando evento:`, err.message);
    }
  }

  @EventPattern('activation.requested')
  async handleActivationRequested(@Payload() message: any) {
    await this.persistEvent(message, 'activation.requested');
  }

  @EventPattern('billing.events')
  async handleBillingEvents(@Payload() message: any) {
    await this.persistEvent(message, 'billing.events');
  }

  @EventPattern('provisioning.events')
  async handleProvisioningEvents(@Payload() message: any) {
    await this.persistEvent(message, 'provisioning.events');
  }

  @EventPattern('activation.events')
  async handleActivationEvents(@Payload() message: any) {
    await this.persistEvent(message, 'activation.events');
  }
}