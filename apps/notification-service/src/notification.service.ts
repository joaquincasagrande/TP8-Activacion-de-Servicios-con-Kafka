import { Injectable, Logger } from '@nestjs/common';
import { InjectModel } from '@nestjs/mongoose';
import { Model } from 'mongoose';
import * as nodemailer from 'nodemailer';
import {
  EVENT_TYPES,
  parseEventEnvelope,
} from '@app/contracts';
import {
  NotificationRecord,
  NotificationRecordDocument,
} from './schemas/notification.schema';

@Injectable()
export class NotificationService {
  private readonly logger = new Logger(NotificationService.name);
  private readonly transporter: nodemailer.Transporter;
  private readonly fromAddress: string;

  constructor(
    @InjectModel(NotificationRecord.name)
    private readonly notificationModel: Model<NotificationRecordDocument>,
  ) {
    const smtpHost = process.env.SMTP_HOST ?? 'localhost';
    const smtpPort = Number(process.env.SMTP_PORT ?? 1025);
    this.fromAddress =
      process.env.SMTP_FROM ?? '"Telecom Activaciones" <no-reply@telecom-poc.local>';

    this.transporter = nodemailer.createTransport({
      host: smtpHost,
      port: smtpPort,
      secure: false,
      ignoreTLS: true,
    });
  }

  /**
   * RF-07: Consume activation.events en el grupo notification-svc.
   * Envía email de bienvenida (ActivationCompleted) o de error (ActivationFailed) a Mailhog.
   */
  async handleActivationEvent(raw: any): Promise<void> {
    const envelope = parseEventEnvelope<Record<string, any>>(raw);
    const { eventId, eventType, correlationId, customerId, payload } = envelope;

    if (
      eventType !== EVENT_TYPES.ACTIVATION_COMPLETED &&
      eventType !== EVENT_TYPES.ACTIVATION_FAILED
    ) {
      return;
    }

    // Verificar idempotencia por activationId + eventType
    const alreadySent = await this.notificationModel
      .findOne({ activationId: correlationId, eventType })
      .lean();

    if (alreadySent) {
      this.logger.warn(
        `⏭️ [${correlationId}] Notificación para ${eventType} ya fue enviada previamente. Omitiendo duplicado.`,
      );
      return;
    }

    const planId = payload?.planId ?? 'Plan Contratado';
    const recipientEmail = `${customerId.toLowerCase().replace(/[^a-z0-9]/g, '')}@cliente.local`;
    const isSuccess = eventType === EVENT_TYPES.ACTIVATION_COMPLETED;

    const subject = isSuccess
      ? `✅ ¡Bienvenido! Tu plan ${planId} ya está ACTIVO (${correlationId})`
      : `⚠️ Problema al activar tu plan ${planId} (${correlationId})`;

    const htmlBody = isSuccess
      ? `
        <div style="font-family: Arial, sans-serif; padding: 20px; border: 2px solid #16a34a; border-radius: 8px;">
          <h2 style="color: #16a34a;">¡Hola ${customerId}! Tu servicio ya está activo 🎉</h2>
          <p>Te confirmamos que la activación de tu plan <strong>${planId}</strong> se completó exitosamente.</p>
          <ul>
            <li><strong>ID de Activación:</strong> ${correlationId}</li>
            <li><strong>Cliente:</strong> ${customerId}</li>
            <li><strong>Estado Final:</strong> ACTIVE</li>
            <li><strong>Fecha:</strong> ${payload?.completedAt ?? envelope.occurredAt}</li>
          </ul>
          <p style="color: #6b7280; font-size: 12px;">Evento Kafka: ${eventType} (${eventId})</p>
        </div>
      `
      : `
        <div style="font-family: Arial, sans-serif; padding: 20px; border: 2px solid #dc2626; border-radius: 8px;">
          <h2 style="color: #dc2626;">Hola ${customerId}, no pudimos completar tu activación</h2>
          <p>Ocurrió un inconveniente técnico al procesar la contratación del plan <strong>${planId}</strong>.</p>
          <ul>
            <li><strong>ID de Activación:</strong> ${correlationId}</li>
            <li><strong>Etapa con fallo:</strong> ${payload?.failedStep ?? 'N/A'}</li>
            <li><strong>Motivo:</strong> ${payload?.reason ?? 'Error en aprovisionamiento/facturación'}</li>
            <li><strong>Compensación:</strong> Cualquier cargo o cuenta de facturación asociada ha sido anulada automáticamente.</li>
          </ul>
          <p style="color: #6b7280; font-size: 12px;">Evento Kafka: ${eventType} (${eventId})</p>
        </div>
      `;

    await this.transporter.sendMail({
      from: this.fromAddress,
      to: recipientEmail,
      subject,
      html: htmlBody,
    });

    await this.notificationModel.create({
      activationId: correlationId,
      eventId,
      eventType,
      customerId,
      recipientEmail,
      subject,
      body: htmlBody,
      status: 'SENT',
      sentAt: new Date().toISOString(),
    });

    this.logger.log(
      `📧 [${correlationId}] Email enviado a ${recipientEmail} vía Mailhog (${eventType})`,
    );
  }
}
