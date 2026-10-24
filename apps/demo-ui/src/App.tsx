import { useEffect, useRef, useState } from 'react';
import { io, Socket } from 'socket.io-client';
import {
  Play,
  AlertTriangle,
  CheckCircle2,
  XCircle,
  Clock,
  Mail,
  Database,
  RotateCcw,
  Wifi,
  WifiOff,
  Activity,
} from 'lucide-react';

const API_URL = import.meta.env.VITE_API_URL ?? 'http://localhost:3000';

interface EventItem {
  id: string;
  topic: string;
  eventType: string;
  correlationId: string;
  customerId: string;
  source?: string;
  status: 'PENDING' | 'COMPLETED' | 'FAILED' | 'COMPENSATED';
  payload: any;
  timestamp: string;
}

interface ActivationSummary {
  activationId: string;
  customerId: string;
  planId: string;
  status: 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';
  simulateFailure: string;
  steps?: {
    billing?: { status: string; at?: string | null; reason?: string };
    provisioning?: { status: string; at?: string | null; reason?: string };
  };
}

function resolveTopicAndStatus(eventType: string): {
  topic: string;
  status: EventItem['status'];
} {
  switch (eventType) {
    case 'ActivationRequested':
      return { topic: 'activation.requested', status: 'PENDING' };
    case 'BillingAccountCreated':
      return { topic: 'billing.events', status: 'COMPLETED' };
    case 'BillingFailed':
      return { topic: 'billing.events', status: 'FAILED' };
    case 'BillingAccountCancelled':
      return { topic: 'billing.events', status: 'COMPENSATED' };
    case 'ProvisioningCompleted':
      return { topic: 'provisioning.events', status: 'COMPLETED' };
    case 'ProvisioningFailed':
      return { topic: 'provisioning.events', status: 'FAILED' };
    case 'ActivationCompleted':
      return { topic: 'activation.events', status: 'COMPLETED' };
    case 'ActivationFailed':
      return { topic: 'activation.events', status: 'FAILED' };
    default:
      return { topic: 'activation.events', status: 'PENDING' };
  }
}

export default function App() {
  const [events, setEvents] = useState<EventItem[]>([]);
  const [customerId, setCustomerId] = useState('C-8921');
  const [planId, setPlanId] = useState('PLAN_FIBRA_500');
  const [isSimulating, setIsSimulating] = useState(false);
  const [wsConnected, setWsConnected] = useState(false);
  const [activeSaga, setActiveSaga] = useState<ActivationSummary | null>(null);
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const seenEventIdsRef = useRef<Set<string>>(new Set());
  const socketRef = useRef<Socket | null>(null);

  useEffect(() => {
    const socket = io(API_URL, {
      transports: ['websocket', 'polling'],
    });
    socketRef.current = socket;

    socket.on('connect', () => {
      setWsConnected(true);
    });

    socket.on('disconnect', () => {
      setWsConnected(false);
    });

    // Recibe cada sobre individual publicado/consumido en la Saga
    socket.on('activation.event', (envelope: any) => {
      if (!envelope || !envelope.eventId) return;
      if (seenEventIdsRef.current.has(envelope.eventId)) return;
      seenEventIdsRef.current.add(envelope.eventId);

      const { topic, status } = resolveTopicAndStatus(envelope.eventType);
      const newEvt: EventItem = {
        id: envelope.eventId,
        topic,
        eventType: envelope.eventType,
        correlationId: envelope.correlationId,
        customerId: envelope.customerId,
        source: envelope.source,
        status,
        payload: envelope.payload,
        timestamp: envelope.occurredAt
          ? new Date(envelope.occurredAt).toLocaleTimeString()
          : new Date().toLocaleTimeString(),
      };
      setEvents((prev) => [newEvt, ...prev]);
    });

    // Recibe la actualización consolidada del documento Activation en MongoDB
    socket.on('activation.updated', (update: any) => {
      if (!update || !update.activationId) return;

      setActiveSaga({
        activationId: update.activationId,
        customerId: update.customerId,
        planId: update.planId,
        status: update.status,
        simulateFailure: update.simulateFailure,
        steps: update.steps,
      });

      // Sincroniza cualquier evento de history que no haya llegado suelto (ej. BillingAccountCancelled)
      if (Array.isArray(update.history)) {
        update.history.forEach((h: any) => {
          const evtId = h.eventId || `${update.activationId}-${h.eventType}-${h.at}`;
          if (seenEventIdsRef.current.has(evtId)) return;
          seenEventIdsRef.current.add(evtId);

          const { topic, status } = resolveTopicAndStatus(h.eventType);
          const item: EventItem = {
            id: evtId,
            topic,
            eventType: h.eventType,
            correlationId: update.activationId,
            customerId: update.customerId,
            source: h.source,
            status,
            payload: h.payload,
            timestamp: h.at
              ? new Date(h.at).toLocaleTimeString()
              : new Date().toLocaleTimeString(),
          };
          setEvents((prev) => [item, ...prev]);
        });
      }

      if (update.status === 'ACTIVE' || update.status === 'FAILED') {
        setIsSimulating(false);
      }
    });

    return () => {
      socket.disconnect();
    };
  }, []);

  const triggerFlow = async (
    scenario: 'HAPPY_PATH' | 'BILLING_FAIL' | 'PROV_FAIL',
  ) => {
    setIsSimulating(true);
    setErrorMsg(null);

    const simulateFailure =
      scenario === 'BILLING_FAIL'
        ? 'billing'
        : scenario === 'PROV_FAIL'
          ? 'provisioning'
          : 'none';

    try {
      const response = await fetch(`${API_URL}/activations`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customerId: customerId.trim() || 'C-100',
          planId,
          channel: 'web',
          simulateFailure,
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }

      const data = await response.json();
      setActiveSaga({
        activationId: data.activationId,
        customerId: data.customerId,
        planId: data.planId,
        status: data.status,
        simulateFailure: data.simulateFailure,
      });

      if (socketRef.current && data.activationId) {
        socketRef.current.emit('subscribe', { activationId: data.activationId });
      }

      // Fallback de seguridad para liberar el botón si termina la Saga
      setTimeout(() => setIsSimulating(false), 3800);
    } catch (err: any) {
      setErrorMsg(
        `No se pudo conectar con activation-api en ${API_URL} (${err?.message ?? 'Error'}). Verifica que el backend esté corriendo.`,
      );
      setIsSimulating(false);
    }
  };

  const clearConsole = () => {
    seenEventIdsRef.current.clear();
    setEvents([]);
    setActiveSaga(null);
    setErrorMsg(null);
  };

  return (
    <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
      <header
        style={{
          marginBottom: '24px',
          borderBottom: '1px solid #1e293b',
          paddingBottom: '16px',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: '12px',
        }}
      >
        <div>
          <h1 style={{ fontSize: '24px', fontWeight: 'bold', color: '#f8fafc' }}>
            MegaTelco &bull; Consola de Activaciones y Coreografía Kafka
          </h1>
          <p style={{ color: '#94a3b8', fontSize: '14px', marginTop: '4px' }}>
            Flujo de eventos distribuidos en tiempo real (REST 202 + Kafka KRaft + WebSocket + Saga).
          </p>
        </div>

        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: '8px',
            padding: '6px 12px',
            borderRadius: '999px',
            fontSize: '12px',
            fontWeight: 600,
            background: wsConnected ? 'rgba(22, 163, 74, 0.15)' : 'rgba(220, 38, 38, 0.15)',
            border: `1px solid ${wsConnected ? '#16a34a' : '#dc2626'}`,
            color: wsConnected ? '#4ade80' : '#f87171',
          }}
        >
          {wsConnected ? <Wifi size={14} /> : <WifiOff size={14} />}
          {wsConnected ? 'WebSocket En Vivo (:3000)' : 'WebSocket Desconectado'}
        </div>
      </header>

      {errorMsg && (
        <div
          style={{
            background: 'rgba(220, 38, 38, 0.15)',
            border: '1px solid #dc2626',
            color: '#fca5a5',
            padding: '12px 16px',
            borderRadius: '8px',
            marginBottom: '16px',
            fontSize: '14px',
          }}
        >
          {errorMsg}
        </div>
      )}

      {/* Barra de Control */}
      <div
        style={{
          background: '#131c2e',
          padding: '18px',
          borderRadius: '8px',
          border: '1px solid #1e293b',
          display: 'flex',
          gap: '16px',
          flexWrap: 'wrap',
          alignItems: 'center',
          marginBottom: '24px',
        }}
      >
        <div>
          <label
            style={{
              display: 'block',
              fontSize: '12px',
              color: '#94a3b8',
              marginBottom: '4px',
            }}
          >
            ID Cliente (Partition Key)
          </label>
          <input
            value={customerId}
            onChange={(e) => setCustomerId(e.target.value)}
            style={{
              background: '#0b0f19',
              border: '1px solid #334155',
              color: '#fff',
              padding: '8px 12px',
              borderRadius: '6px',
            }}
          />
        </div>

        <div>
          <label
            style={{
              display: 'block',
              fontSize: '12px',
              color: '#94a3b8',
              marginBottom: '4px',
            }}
          >
            Plan
          </label>
          <select
            value={planId}
            onChange={(e) => setPlanId(e.target.value)}
            style={{
              background: '#0b0f19',
              border: '1px solid #334155',
              color: '#fff',
              padding: '8px 12px',
              borderRadius: '6px',
            }}
          >
            <option value="PLAN_FIBRA_300">Fibra 300 Mbps</option>
            <option value="PLAN_FIBRA_500">Fibra 500 Mbps Pro</option>
            <option value="PLAN_GIGA_CORP">Giga Corporativo 1Gbps</option>
          </select>
        </div>

        <div
          style={{
            display: 'flex',
            gap: '10px',
            marginLeft: 'auto',
            flexWrap: 'wrap',
          }}
        >
          <button
            disabled={isSimulating}
            onClick={() => triggerFlow('HAPPY_PATH')}
            style={{
              background: '#16a34a',
              color: '#fff',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              opacity: isSimulating ? 0.6 : 1,
            }}
          >
            <Play size={16} /> Flujo Exitoso
          </button>

          <button
            disabled={isSimulating}
            onClick={() => triggerFlow('BILLING_FAIL')}
            style={{
              background: '#d97706',
              color: '#fff',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              opacity: isSimulating ? 0.6 : 1,
            }}
          >
            <AlertTriangle size={16} /> Fallo Facturación
          </button>

          <button
            disabled={isSimulating}
            onClick={() => triggerFlow('PROV_FAIL')}
            style={{
              background: '#dc2626',
              color: '#fff',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              opacity: isSimulating ? 0.6 : 1,
            }}
          >
            <XCircle size={16} /> Fallo Red (Compensación)
          </button>

          <button
            onClick={clearConsole}
            style={{
              background: '#334155',
              color: '#e2e8f0',
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
            }}
          >
            <RotateCcw size={16} /> Limpiar
          </button>
        </div>
      </div>

      {/* Estado de la Activación Actual */}
      {activeSaga && (
        <div
          style={{
            background: '#0f172a',
            border: '1px solid #1e293b',
            borderRadius: '8px',
            padding: '14px 18px',
            marginBottom: '24px',
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: '12px',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <Activity size={18} color="#38bdf8" />
            <span style={{ fontSize: '14px', color: '#e2e8f0' }}>
              Activación: <b>{activeSaga.activationId}</b> &bull; Cliente:{' '}
              <b>{activeSaga.customerId}</b>
            </span>
          </div>

          <div style={{ display: 'flex', gap: '12px', alignItems: 'center', fontSize: '13px' }}>
            <span style={{ color: '#94a3b8' }}>
              Billing:{' '}
              <b
                style={{
                  color:
                    activeSaga.steps?.billing?.status === 'OK'
                      ? '#4ade80'
                      : activeSaga.steps?.billing?.status === 'CANCELLED'
                        ? '#fbbf24'
                        : activeSaga.steps?.billing?.status === 'FAILED'
                          ? '#f87171'
                          : '#38bdf8',
                }}
              >
                {activeSaga.steps?.billing?.status ?? 'PENDING'}
              </b>
            </span>
            <span style={{ color: '#94a3b8' }}>
              Provisioning:{' '}
              <b
                style={{
                  color:
                    activeSaga.steps?.provisioning?.status === 'OK'
                      ? '#4ade80'
                      : activeSaga.steps?.provisioning?.status === 'FAILED'
                        ? '#f87171'
                        : '#38bdf8',
                }}
              >
                {activeSaga.steps?.provisioning?.status ?? 'PENDING'}
              </b>
            </span>
            <span
              style={{
                padding: '4px 10px',
                borderRadius: '6px',
                fontWeight: 700,
                background:
                  activeSaga.status === 'ACTIVE'
                    ? '#16a34a'
                    : activeSaga.status === 'FAILED'
                      ? '#dc2626'
                      : '#0284c7',
                color: '#fff',
              }}
            >
              {activeSaga.status}
            </span>
          </div>
        </div>
      )}

      {/* Grid de Monitoreo */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '24px' }}>
        <div>
          <h2
            style={{
              fontSize: '18px',
              marginBottom: '14px',
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
            }}
          >
            <Clock size={20} color="#38bdf8" /> Stream de Eventos en Topics (Tiempo Real)
          </h2>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            {events.length === 0 ? (
              <div
                style={{
                  padding: '40px',
                  textAlign: 'center',
                  background: '#0f172a',
                  borderRadius: '8px',
                  border: '1px dashed #334155',
                  color: '#64748b',
                }}
              >
                Sin eventos registrados. Ejecuta uno de los escenarios para disparar el POST /activations y recibir los eventos de Kafka vía WebSocket.
              </div>
            ) : (
              events.map((evt) => {
                const borderColor =
                  evt.status === 'COMPLETED'
                    ? '#22c55e'
                    : evt.status === 'FAILED'
                      ? '#ef4444'
                      : evt.status === 'COMPENSATED'
                        ? '#f59e0b'
                        : '#38bdf8';

                return (
                  <div
                    key={evt.id}
                    style={{
                      background: '#0f172a',
                      border: '1px solid #1e293b',
                      borderLeft: `4px solid ${borderColor}`,
                      borderRadius: '6px',
                      padding: '12px 14px',
                      display: 'flex',
                      flexDirection: 'column',
                      gap: '6px',
                    }}
                  >
                    <div
                      style={{
                        display: 'flex',
                        justifyContent: 'space-between',
                        alignItems: 'center',
                      }}
                    >
                      <span
                        style={{
                          fontSize: '12px',
                          background: '#1e293b',
                          padding: '2px 8px',
                          borderRadius: '4px',
                          color: '#94a3b8',
                        }}
                      >
                        Topic: <b>{evt.topic}</b>{' '}
                        {evt.source ? `• Source: ${evt.source}` : ''}
                      </span>
                      <span style={{ fontSize: '12px', color: '#64748b' }}>
                        {evt.timestamp}
                      </span>
                    </div>

                    <div
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px',
                        fontSize: '15px',
                        fontWeight: 'bold',
                      }}
                    >
                      {evt.status === 'COMPLETED' ? (
                        <CheckCircle2 size={16} color="#22c55e" />
                      ) : evt.status === 'FAILED' ? (
                        <XCircle size={16} color="#ef4444" />
                      ) : evt.status === 'COMPENSATED' ? (
                        <AlertTriangle size={16} color="#f59e0b" />
                      ) : (
                        <Clock size={16} color="#38bdf8" />
                      )}
                      <span>{evt.eventType}</span>
                      <span
                        style={{
                          fontSize: '12px',
                          color: '#64748b',
                          fontWeight: 'normal',
                        }}
                      >
                        (CorrId: {evt.correlationId} • Cliente: {evt.customerId})
                      </span>
                    </div>

                    <pre
                      style={{
                        background: '#070b12',
                        padding: '8px',
                        borderRadius: '4px',
                        fontSize: '12px',
                        color: '#93c5fd',
                        overflowX: 'auto',
                      }}
                    >
                      {JSON.stringify(evt.payload, null, 2)}
                    </pre>
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Panel lateral informativo */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          <div
            style={{
              background: '#131c2e',
              border: '1px solid #1e293b',
              borderRadius: '8px',
              padding: '16px',
            }}
          >
            <h3
              style={{
                fontSize: '16px',
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                marginBottom: '8px',
              }}
            >
              <Mail size={18} color="#a855f7" /> Notification Service
            </h3>
            <p style={{ fontSize: '13px', color: '#94a3b8', lineHeight: '1.5' }}>
              Consume <code style={{ color: '#c084fc' }}>activation.events</code>.
              Envía email vía Mailhog con verificación de idempotencia en MongoDB.
            </p>
            <div style={{ marginTop: '10px' }}>
              <a
                href="http://localhost:8025"
                target="_blank"
                rel="noreferrer"
                style={{
                  color: '#38bdf8',
                  fontSize: '13px',
                  textDecoration: 'none',
                }}
              >
                &rarr; Ver Mailhog Web (8025)
              </a>
            </div>
          </div>

          <div
            style={{
              background: '#131c2e',
              border: '1px solid #1e293b',
              borderRadius: '8px',
              padding: '16px',
            }}
          >
            <h3
              style={{
                fontSize: '16px',
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                marginBottom: '8px',
              }}
            >
              <Database size={18} color="#10b981" /> CRM Analytics Service
            </h3>
            <p style={{ fontSize: '13px', color: '#94a3b8', lineHeight: '1.5' }}>
              Auditoría desacoplada consumiendo los 4 tópicos de negocio en la
              colección <code style={{ color: '#34d399' }}>event_log</code>.
            </p>
            <div style={{ marginTop: '10px' }}>
              <a
                href="http://localhost:8080"
                target="_blank"
                rel="noreferrer"
                style={{
                  color: '#38bdf8',
                  fontSize: '13px',
                  textDecoration: 'none',
                }}
              >
                &rarr; Ver Kafka UI (8080)
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
