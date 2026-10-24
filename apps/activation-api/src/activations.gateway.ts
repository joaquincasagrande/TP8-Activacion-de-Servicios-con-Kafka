import { Logger } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { EventEnvelope } from '@app/contracts';
import { Activation } from './schemas/activation.schema';

@WebSocketGateway({
  cors: {
    origin: '*',
  },
})
export class ActivationsGateway
  implements OnGatewayConnection, OnGatewayDisconnect
{
  private readonly logger = new Logger(ActivationsGateway.name);

  @WebSocketServer()
  server: Server;

  handleConnection(client: Socket): void {
    this.logger.log(`🔌 Cliente WebSocket conectado: ${client.id}`);
  }

  handleDisconnect(client: Socket): void {
    this.logger.log(`🔌 Cliente WebSocket desconectado: ${client.id}`);
  }

  /**
   * Permite a la UI suscribirse a una activación específica por su ID (opcional),
   * además del broadcast general.
   */
  @SubscribeMessage('subscribe')
  handleSubscribe(
    @MessageBody() data: { activationId?: string } | string,
    @ConnectedSocket() client: Socket,
  ): { subscribed: string } {
    const activationId =
      typeof data === 'string' ? data : data?.activationId ?? '';
    if (activationId) {
      client.join(activationId);
      this.logger.log(`Cliente ${client.id} suscrito a ${activationId}`);
    }
    return { subscribed: activationId };
  }

  /**
   * Emite el estado actualizado de la activación y el último evento ocurrido
   * a todos los clientes conectados en tiempo real (RF-08).
   */
  emitActivationUpdate(
    activation: Activation,
    event?: EventEnvelope<any>,
  ): void {
    if (!this.server) return;

    const payload = {
      activationId: activation._id,
      customerId: activation.customerId,
      planId: activation.planId,
      status: activation.status,
      simulateFailure: activation.simulateFailure,
      steps: activation.steps,
      history: activation.history,
      latestEvent: event ?? null,
      updatedAt: new Date().toISOString(),
    };

    // Emitir globalmente y a la sala específica del activationId
    this.server.emit('activation.updated', payload);
    this.server.to(activation._id).emit('activation.updated', payload);

    if (event) {
      this.server.emit('activation.event', event);
      this.server.to(activation._id).emit('activation.event', event);
    }
  }
}
