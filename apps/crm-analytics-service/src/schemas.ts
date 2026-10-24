import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { Document } from 'mongoose';

@Schema({ collection: 'event_log', timestamps: true })
export class EventLog extends Document {
    @Prop({ required: true, unique: true })
    eventId: string;

    @Prop({ required: true })
    eventType: string;

    @Prop({ required: true })
    correlationId: string;

    @Prop({ required: true })
    customerId: string;

    @Prop({ required: true })
    source: string;

    @Prop({ type: Object })
    payload: Record<string, any>;

    @Prop({ required: true })
    occurredAt: string;
}

export const EventLogSchema = SchemaFactory.createForClass(EventLog);
EventLogSchema.index({ correlationId: 1 });