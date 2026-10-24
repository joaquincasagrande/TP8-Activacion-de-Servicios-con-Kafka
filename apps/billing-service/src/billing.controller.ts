import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { KAFKA_TOPICS } from '@app/contracts';
import { BillingService } from './billing.service';

@Controller()
export class BillingController {
  constructor(private readonly billingService: BillingService) {}

  @EventPattern(KAFKA_TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(@Payload() message: any): Promise<void> {
    await this.billingService.handleActivationRequested(message);
  }

  @EventPattern(KAFKA_TOPICS.ACTIVATION_EVENTS)
  async onActivationEvent(@Payload() message: any): Promise<void> {
    await this.billingService.handleActivationEvent(message);
  }
}
