import {
  Body,
  Controller,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Post,
} from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { KAFKA_TOPICS } from '@app/contracts';
import { ActivationsService } from './activations.service';
import { CreateActivationDto } from './dto/create-activation.dto';

@Controller('activations')
export class ActivationsController {
  constructor(private readonly activationsService: ActivationsService) {}

  /**
   * RF-01, RF-02: Crea una activación y responde inmediatamente HTTP 202 Accepted
   * con el activationId y estado PENDING.
   */
  @Post()
  @HttpCode(HttpStatus.ACCEPTED)
  async createActivation(@Body() dto: CreateActivationDto) {
    return this.activationsService.createActivation(dto);
  }

  /**
   * Lista las activaciones recientes (soporte adicional para la UI).
   */
  @Get()
  async listActivations(): Promise<Array<Record<string, any>>> {
    return this.activationsService.listActivations();
  }

  /**
   * RF-09: Obtiene el estado actual y el historial de eventos de una activación.
   */
  @Get(':id')
  async getActivation(@Param('id') id: string): Promise<Record<string, any>> {
    return this.activationsService.getActivationById(id);
  }

  /**
   * Agregador de la Saga: consume resultados desde billing.events
   */
  @EventPattern(KAFKA_TOPICS.BILLING_EVENTS)
  async onBillingEvent(@Payload() message: any): Promise<void> {
    await this.activationsService.handleBillingEvent(message);
  }

  /**
   * Agregador de la Saga: consume resultados desde provisioning.events
   */
  @EventPattern(KAFKA_TOPICS.PROVISIONING_EVENTS)
  async onProvisioningEvent(@Payload() message: any): Promise<void> {
    await this.activationsService.handleProvisioningEvent(message);
  }
}
