import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { KAFKA_TOPICS } from '@app/contracts';
import { CrmService } from './crm.service';

@Controller()
export class CrmController {
  constructor(private readonly crmService: CrmService) {}

  @EventPattern(KAFKA_TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(@Payload() message: any): Promise<void> {
    await this.crmService.recordEvent(
      KAFKA_TOPICS.ACTIVATION_REQUESTED,
      message,
    );
  }

  @EventPattern(KAFKA_TOPICS.BILLING_EVENTS)
  async onBillingEvent(@Payload() message: any): Promise<void> {
    await this.crmService.recordEvent(KAFKA_TOPICS.BILLING_EVENTS, message);
  }

  @EventPattern(KAFKA_TOPICS.PROVISIONING_EVENTS)
  async onProvisioningEvent(@Payload() message: any): Promise<void> {
    await this.crmService.recordEvent(
      KAFKA_TOPICS.PROVISIONING_EVENTS,
      message,
    );
  }

  @EventPattern(KAFKA_TOPICS.ACTIVATION_EVENTS)
  async onActivationEvent(@Payload() message: any): Promise<void> {
    await this.crmService.recordEvent(KAFKA_TOPICS.ACTIVATION_EVENTS, message);
  }
}
