export const TOPICS = {
    ACTIVATION_REQUESTED: 'activation.requested',
    BILLING_EVENTS: 'billing.events',
    PROVISIONING_EVENTS: 'provisioning.events',
    ACTIVATION_EVENTS: 'activation.events',
} as const;

export interface EventEnvelope<T = any> {
    eventId: string;
    eventType: string;
    version: number;
    occurredAt: string;
    correlationId: string; // activationId
    customerId: string;    // clave de partición
    source: string;
    payload: T;
}