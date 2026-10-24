import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { KAFKA_TOPICS } from '@app/contracts';
import { NotificationService } from './notification.service';

@Controller()
export class NotificationController {
  constructor(private readonly notificationService: NotificationService) {}

  @EventPattern(KAFKA_TOPICS.ACTIVATION_EVENTS)
  async onActivationEvent(@Payload() message: any): Promise<void> {
    await this.notificationService.handleActivationEvent(message);
  }
}
