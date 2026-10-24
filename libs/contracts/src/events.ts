export const EVENT_TYPES = {
  // activation.requested
  ACTIVATION_REQUESTED: 'ActivationRequested',

  // billing.events
  BILLING_ACCOUNT_CREATED: 'BillingAccountCreated',
  BILLING_FAILED: 'BillingFailed',
  BILLING_ACCOUNT_CANCELLED: 'BillingAccountCancelled',

  // provisioning.events
  PROVISIONING_COMPLETED: 'ProvisioningCompleted',
  PROVISIONING_FAILED: 'ProvisioningFailed',

  // activation.events
  ACTIVATION_COMPLETED: 'ActivationCompleted',
  ACTIVATION_FAILED: 'ActivationFailed',
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];

export const ACTIVATION_STATUS = {
  PENDING: 'PENDING',
  IN_PROGRESS: 'IN_PROGRESS',
  ACTIVE: 'ACTIVE',
  FAILED: 'FAILED',
} as const;

export type ActivationStatus =
  (typeof ACTIVATION_STATUS)[keyof typeof ACTIVATION_STATUS];

export const STEP_STATUS = {
  PENDING: 'PENDING',
  OK: 'OK',
  FAILED: 'FAILED',
  CANCELLED: 'CANCELLED',
} as const;

export type StepStatus = (typeof STEP_STATUS)[keyof typeof STEP_STATUS];

export const SIMULATE_FAILURE_MODES = {
  NONE: 'none',
  BILLING: 'billing',
  PROVISIONING: 'provisioning',
} as const;

export type SimulateFailureMode =
  (typeof SIMULATE_FAILURE_MODES)[keyof typeof SIMULATE_FAILURE_MODES];

export const SERVICE_SOURCES = {
  ACTIVATION_API: 'activation-api',
  BILLING_SERVICE: 'billing-service',
  PROVISIONING_SERVICE: 'provisioning-service',
  NOTIFICATION_SERVICE: 'notification-service',
  CRM_ANALYTICS_SERVICE: 'crm-analytics-service',
} as const;

export type ServiceSource =
  (typeof SERVICE_SOURCES)[keyof typeof SERVICE_SOURCES];
