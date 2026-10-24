import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ collection: 'notifications', timestamps: true })
export class NotificationLog extends Document {
    @Prop({ required: true }) activationId: string;
    @Prop({ required: true }) eventType: string;
    @Prop({ required: true }) customerId: string;
    @Prop({ required: true }) subject: string;
}
export const NotificationSchema = SchemaFactory.createForClass(NotificationLog);
NotificationSchema.index({ activationId: 1, eventType: 1 });

@Schema({ collection: 'processed_events' })
export class ProcessedEvent extends Document {
    @Prop({ required: true, unique: true }) eventId: string;
    @Prop({ default: Date.now, expires: '7d' }) processedAt: Date;
}
export const ProcessedEventSchema = SchemaFactory.createForClass(ProcessedEvent);