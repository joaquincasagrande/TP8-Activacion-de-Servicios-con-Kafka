import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';

export type BillingAccountDocument = HydratedDocument<BillingAccount>;

export type BillingAccountStatus = 'CREATED' | 'CANCELLED' | 'FAILED';

@Schema({ collection: 'billing_accounts', timestamps: true, versionKey: false })
export class BillingAccount {
  @Prop({ type: String, required: true, unique: true, index: true })
  activationId: string;

  @Prop({ type: String, required: true })
  accountId: string;

  @Prop({ type: String, required: true, index: true })
  customerId: string;

  @Prop({ type: String, required: true })
  planId: string;

  @Prop({
    type: String,
    required: true,
    enum: ['CREATED', 'CANCELLED', 'FAILED'],
  })
  status: BillingAccountStatus;

  @Prop({ type: String })
  reason?: string;
}

export const BillingAccountSchema =
  SchemaFactory.createForClass(BillingAccount);
