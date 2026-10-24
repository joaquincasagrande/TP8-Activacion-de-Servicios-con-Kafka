import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type EventLogDocument = HydratedDocument<EventLog>;

@Schema({ collection: 'event_log', timestamps: true, versionKey: false })
export class EventLog {
  @Prop({ type: String, required: true, unique: true, index: true })
  eventId: string;

  @Prop({ type: String, required: true, index: true })
  correlationId: string;

  @Prop({ type: String, required: true, index: true })
  customerId: string;

  @Prop({ type: String, required: true, index: true })
  topic: string;

  @Prop({ type: String, required: true })
  eventType: string;

  @Prop({ type: Number, default: 1 })
  version: number;

  @Prop({ type: String, required: true })
  source: string;

  @Prop({ type: String, required: true })
  occurredAt: string;

  @Prop({ type: Object })
  envelope: Record<string, any>;
}

export const EventLogSchema = SchemaFactory.createForClass(EventLog);
