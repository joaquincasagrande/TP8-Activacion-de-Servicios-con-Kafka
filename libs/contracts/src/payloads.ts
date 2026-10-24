import { SimulateFailureMode } from './events';

export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  simulateFailure: SimulateFailureMode;
}

export interface BillingEventPayload {
  accountId?: string;
  planId?: string;
  status: 'CREATED' | 'FAILED' | 'CANCELLED';
  reason?: string;
  simulateFailure?: SimulateFailureMode;
}

export interface ProvisioningEventPayload {
  orderId?: string;
  planId?: string;
  status: 'COMPLETED' | 'FAILED';
  durationMs?: number;
  reason?: string;
  simulateFailure?: SimulateFailureMode;
}

export interface ActivationCompletedPayload {
  planId: string;
  completedAt: string;
}

export interface ActivationFailedPayload {
  planId?: string;
  failedStep: 'billing' | 'provisioning';
  reason: string;
  failedAt: string;
}
