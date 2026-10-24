import { Prop, Schema, SchemaFactory } from '@nestjs/mongoose';
import { HydratedDocument } from 'mongoose';
import {
  ACTIVATION_STATUS,
  ActivationStatus,
  SIMULATE_FAILURE_MODES,
  SimulateFailureMode,
  STEP_STATUS,
  StepStatus,
} from '@app/contracts';

export type ActivationDocument = HydratedDocument<Activation>;

export interface ActivationStepInfo {
  status: StepStatus;
  at: string | null;
  reason?: string;
}

export interface ActivationHistoryItem {
  eventId?: string;
  eventType: string;
  at: string;
  source?: string;
  payload?: Record<string, any>;
}

@Schema({ collection: 'activations', timestamps: true, versionKey: false })
export class Activation {
  @Prop({ type: String, required: true })
  _id: string;

  @Prop({ type: String, required: true, index: true })
  customerId: string;

  @Prop({ type: String, required: true })
  planId: string;

  @Prop({ type: String, default: 'web' })
  channel: string;

  @Prop({
    type: String,
    required: true,
    default: ACTIVATION_STATUS.PENDING,
    index: true,
  })
  status: ActivationStatus;

  @Prop({
    type: String,
    default: SIMULATE_FAILURE_MODES.NONE,
  })
  simulateFailure: SimulateFailureMode;

  @Prop({
    type: Object,
    default: () => ({
      billing: { status: STEP_STATUS.PENDING, at: null },
      provisioning: { status: STEP_STATUS.PENDING, at: null },
    }),
  })
  steps: {
    billing: ActivationStepInfo;
    provisioning: ActivationStepInfo;
  };

  @Prop({
    type: [
      {
        _id: false,
        eventId: String,
        eventType: String,
        at: String,
        source: String,
        payload: Object,
      },
    ],
    default: [],
  })
  history: ActivationHistoryItem[];

  createdAt?: Date;
  updatedAt?: Date;
}

export const ActivationSchema = SchemaFactory.createForClass(Activation);
