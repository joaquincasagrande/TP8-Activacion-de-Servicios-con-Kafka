import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type NotificationRecordDocument = HydratedDocument<NotificationRecord>;

@Schema({ collection: 'notifications', timestamps: true, versionKey: false })
export class NotificationRecord {
  @Prop({ type: String, required: true, index: true })
  activationId: string;

  @Prop({ type: String, required: true })
  eventId: string;

  @Prop({ type: String, required: true })
  eventType: string;

  @Prop({ type: String, required: true, index: true })
  customerId: string;

  @Prop({ type: String, required: true })
  recipientEmail: string;

  @Prop({ type: String, required: true })
  subject: string;

  @Prop({ type: String, required: true })
  body: string;

  @Prop({ type: String, default: 'SENT' })
  status: 'SENT' | 'FAILED';

  @Prop({ type: String, required: true })
  sentAt: string;
}

export const NotificationRecordSchema =
  SchemaFactory.createForClass(NotificationRecord);

// Índice compuesto requerido en el modelo de datos (activationId + eventType)
NotificationRecordSchema.index(
  { activationId: 1, eventType: 1 },
  { unique: true },
);
