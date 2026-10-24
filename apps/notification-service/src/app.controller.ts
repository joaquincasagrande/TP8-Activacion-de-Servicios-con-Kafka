import { Controller } from '@nestjs/common';
import { EventPattern, Payload } from '@nestjs/microservices';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import * as nodemailer from 'nodemailer';
import { NotificationLog, ProcessedEvent } from './schemas';

@Controller()
export class AppController {
  private transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST || 'localhost',
    port: 1025,
    ignoreTLS: true,
  });

  constructor(
    @InjectModel(NotificationLog.name) private notifModel: Model<NotificationLog>,
    @InjectModel(ProcessedEvent.name) private processedModel: Model<ProcessedEvent>,
  ) { }

  @EventPattern('activation.events')
  async handleActivationEvents(@Payload() message: any) {
    const event = message?.value || message;
    if (!event?.eventId) return;

    // 1. Idempotencia: descartar duplicados
    try {
      await this.processedModel.create({ eventId: event.eventId });
    } catch (err: any) {
      if (err.code === 11000) {
        console.log(`[Idempotencia] Evento duplicado ignorado: ${event.eventId}`);
        return;
      }
      throw err;
    }

    // 2. Enviar email simulado a Mailhog
    const isSuccess = event.eventType === 'ActivationCompleted';
    const subject = isSuccess
      ? `¡Bienvenido! Tu plan fue activado (${event.correlationId})`
      : `Error en la activación de tu plan (${event.correlationId})`;

    await this.transporter.sendMail({
      from: '"MegaTelco Activaciones" <no-reply@megatelco.com>',
      to: `${event.customerId}@cliente.com`,
      subject,
      text: `Estado: ${event.eventType}\nActivación: ${event.correlationId}\nCliente: ${event.customerId}\nPayload: ${JSON.stringify(event.payload)}`,
    });

    // 3. Registrar en base de datos
    await this.notifModel.create({
      activationId: event.correlationId,
      eventType: event.eventType,
      customerId: event.customerId,
      subject,
    });

    console.log(`[Email enviado] ${event.correlationId} -> ${subject}`);
  }
}