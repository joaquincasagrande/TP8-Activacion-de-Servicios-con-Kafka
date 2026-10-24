import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { KAFKA_TOPICS } from '@app/contracts';
import { ProvisioningService } from './provisioning.service';

@Controller()
export class ProvisioningController {
  constructor(private readonly provisioningService: ProvisioningService) {}

  @EventPattern(KAFKA_TOPICS.ACTIVATION_REQUESTED)
  async onActivationRequested(@Payload() message: any): Promise<void> {
    await this.provisioningService.handleActivationRequested(message);
  }
}
