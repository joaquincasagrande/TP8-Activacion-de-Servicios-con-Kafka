export const KAFKA_TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',

  // Dead Letter Queues (DLQ)
  ACTIVATION_REQUESTED_DLQ: 'activation.requested.dlq',
  BILLING_EVENTS_DLQ: 'billing.events.dlq',
  PROVISIONING_EVENTS_DLQ: 'provisioning.events.dlq',
  ACTIVATION_EVENTS_DLQ: 'activation.events.dlq',
} as const;

export type KafkaTopic = (typeof KAFKA_TOPICS)[keyof typeof KAFKA_TOPICS];

export function getDlqTopic(topic: string): string {
  return topic.endsWith('.dlq') ? topic : `${topic}.dlq`;
}

export const CONSUMER_GROUPS = {
  ACTIVATION_API: 'activation-api',
  BILLING_SVC: 'billing-svc',
  PROVISIONING_SVC: 'provisioning-svc',
  NOTIFICATION_SVC: 'notification-svc',
  CRM_ANALYTICS: 'crm-analytics',
} as const;

export type ConsumerGroup = (typeof CONSUMER_GROUPS)[keyof typeof CONSUMER_GROUPS];
