import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type ProvisioningOrderDocument = HydratedDocument<ProvisioningOrder>;

export type ProvisioningOrderStatus = 'COMPLETED' | 'FAILED';

@Schema({
  collection: 'provisioning_orders',
  timestamps: true,
  versionKey: false,
})
export class ProvisioningOrder {
  @Prop({ type: String, required: true, unique: true, index: true })
  activationId: string;

  @Prop({ type: String, required: true })
  orderId: string;

  @Prop({ type: String, required: true, index: true })
  customerId: string;

  @Prop({ type: String, required: true })
  planId: string;

  @Prop({
    type: String,
    required: true,
    enum: ['COMPLETED', 'FAILED'],
  })
  status: ProvisioningOrderStatus;

  @Prop({ type: Number })
  durationMs: number;

  @Prop({ type: String })
  reason?: string;
}

export const ProvisioningOrderSchema =
  SchemaFactory.createForClass(ProvisioningOrder);
