import { randomUUID } from 'crypto';
import { EventType, ServiceSource, SimulateFailureMode } from './events';

/**
 * Sobre común tipado para todos los eventos del sistema (RNF-06, RNF-07).
 * - correlationId: corresponde al activationId para trazabilidad de punta a punta.
 * - customerId: clave de particionado en Kafka para garantizar orden por cliente (RNF-02).
 */
export interface EventEnvelope<TPayload = Record<string, any>> {
  eventId: string;
  eventType: EventType;
  version: number;
  occurredAt: string;
  correlationId: string;
  customerId: string;
  source: ServiceSource | string;
  simulateFailure?: SimulateFailureMode;
  payload: TPayload;
}

export interface CreateEventEnvelopeParams<TPayload> {
  eventType: EventType;
  correlationId: string;
  customerId: string;
  source: ServiceSource | string;
  payload: TPayload;
  simulateFailure?: SimulateFailureMode;
  version?: number;
  eventId?: string;
  occurredAt?: string;
}

/**
 * Fábrica estándar para construir un sobre de evento válido y versionado.
 */
export function createEventEnvelope<TPayload>(
  params: CreateEventEnvelopeParams<TPayload>,
): EventEnvelope<TPayload> {
  return {
    eventId: params.eventId ?? randomUUID(),
    eventType: params.eventType,
    version: params.version ?? 1,
    occurredAt: params.occurredAt ?? new Date().toISOString(),
    correlationId: params.correlationId,
    customerId: params.customerId,
    source: params.source,
    ...(params.simulateFailure
      ? { simulateFailure: params.simulateFailure }
      : {}),
    payload: params.payload,
  };
}

/**
 * Empaqueta un EventEnvelope en el formato esperado por ClientKafka.emit(topic, record)
 * utilizando customerId como clave de partición (RNF-02).
 */
export function toKafkaMessage<TPayload>(envelope: EventEnvelope<TPayload>): {
  key: string;
  value: string;
  headers: Record<string, string>;
} {
  return {
    key: envelope.customerId,
    value: JSON.stringify(envelope),
    headers: {
      eventId: envelope.eventId,
      eventType: envelope.eventType,
      correlationId: envelope.correlationId,
      source: String(envelope.source),
      version: String(envelope.version),
    },
  };
}

/**
 * Deserializa de forma defensiva el payload recibido por @EventPattern en NestJS/KafkaJS,
 * soportando objetos ya parseados, strings JSON, Buffers o estructuras { value: ... }.
 */
export function parseEventEnvelope<TPayload = Record<string, any>>(
  raw: any,
): EventEnvelope<TPayload> {
  if (!raw) {
    throw new Error('Mensaje Kafka vacío o indefinido');
  }

  let candidate = raw;

  if (
    typeof candidate === 'object' &&
    candidate !== null &&
    'value' in candidate &&
    !('eventId' in candidate) &&
    !('eventType' in candidate)
  ) {
    candidate = candidate.value;
  }

  if (Buffer.isBuffer(candidate)) {
    candidate = candidate.toString('utf-8');
  }

  if (typeof candidate === 'string') {
    candidate = JSON.parse(candidate);
  }

  return candidate as EventEnvelope<TPayload>;
}
