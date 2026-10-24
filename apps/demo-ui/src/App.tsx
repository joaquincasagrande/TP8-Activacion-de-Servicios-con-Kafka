import { useState } from 'react'; import {
  Play,
  AlertTriangle,
  CheckCircle2,
  XCircle,
  Clock,
  Mail,
  Database,
  RotateCcw
} from 'lucide-react';

interface EventItem {
  id: string;
  topic: string;
  eventType: string;
  correlationId: string;
  customerId: string;
  status: 'PENDING' | 'COMPLETED' | 'FAILED';
  payload: any;
  timestamp: string;
}

export default function App() {
  const [events, setEvents] = useState<EventItem[]>([]);
  const [customerId, setCustomerId] = useState('C-8921');
  const [planId, setPlanId] = useState('PLAN_FIBRA_500');
  const [isSimulating, setIsSimulating] = useState(false);

  const triggerFlow = (scenario: 'HAPPY_PATH' | 'BILLING_FAIL' | 'PROV_FAIL') => {
    setIsSimulating(true);
    const correlationId = `act-${Math.floor(1000 + Math.random() * 9000)}`;

    const addEventWithDelay = (
      topic: string,
      eventType: string,
      status: 'PENDING' | 'COMPLETED' | 'FAILED',
      payload: any,
      delayMs: number
    ) => {
      setTimeout(() => {
        const newEvt: EventItem = {
          id: `evt-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
          topic,
          eventType,
          correlationId,
          customerId,
          status,
          payload,
          timestamp: new Date().toLocaleTimeString(),
        };
        setEvents((prev) => [newEvt, ...prev]);
      }, delayMs);
    };

    // 1. activation.requested
    addEventWithDelay(
      'activation.requested',
      'ActivationRequested',
      'PENDING',
      { planId, origin: 'demo-ui' },
      200
    );

    // 2. billing.events
    if (scenario === 'BILLING_FAIL') {
      addEventWithDelay(
        'billing.events',
        'BillingValidationFailed',
        'FAILED',
        { reason: 'Fondos insuficientes / Validación bancaria rechazada' },
        1200
      );
      addEventWithDelay(
        'activation.events',
        'ActivationFailed',
        'FAILED',
        { stage: 'billing', notified: true },
        2200
      );
      setTimeout(() => setIsSimulating(false), 2400);
      return;
    }

    addEventWithDelay(
      'billing.events',
      'BillingValidated',
      'COMPLETED',
      { invoiceId: `inv-${Date.now()}` },
      1200
    );

    // 3. provisioning.events
    if (scenario === 'PROV_FAIL') {
      addEventWithDelay(
        'provisioning.events',
        'NetworkConfigFailed',
        'FAILED',
        { reason: 'Timeout en aprovisionamiento de OLT/ONT' },
        2200
      );
      addEventWithDelay(
        'activation.events',
        'ActivationFailed',
        'FAILED',
        { stage: 'provisioning', notified: true },
        3200
      );
      setTimeout(() => setIsSimulating(false), 3400);
      return;
    }

    addEventWithDelay(
      'provisioning.events',
      'NetworkProvisioned',
      'COMPLETED',
      { modemIp: '192.168.100.42', slot: 'GPON-01' },
      2200
    );

    // 4. activation.events (Disparador de Notification Service)
    addEventWithDelay(
      'activation.events',
      'ActivationCompleted',
      'COMPLETED',
      { finalStatus: 'ACTIVE', emailSent: `${customerId}@cliente.com` },
      3200
    );
    setTimeout(() => setIsSimulating(false), 3400);
  };

  return (
    <div style={{ maxWidth: '1200px', margin: '0 auto' }}>
      <header style={{ marginBottom: '24px', borderBottom: '1px solid #1e293b', paddingBottom: '16px' }}>
        <h1 style={{ fontSize: '24px', fontWeight: 'bold', color: '#f8fafc' }}>
          MegaTelco &bull; Consola de Activaciones y Coreografía Kafka
        </h1>
        <p style={{ color: '#94a3b8', fontSize: '14px', marginTop: '4px' }}>
          Flujo de eventos distribuidos, simulación de estados y observabilidad de microservicios.
        </p>
      </header>

      {/* Barra de Control */}
      <div style={{
        background: '#131c2e',
        padding: '18px',
        borderRadius: '8px',
        border: '1px solid #1e293b',
        display: 'flex',
        gap: '16px',
        flexWrap: 'wrap',
        alignItems: 'center',
        marginBottom: '24px'
      }}>
        <div>
          <label style={{ display: 'block', fontSize: '12px', color: '#94a3b8', marginBottom: '4px' }}>ID Cliente</label>
          <input
            value={customerId}
            onChange={(e) => setCustomerId(e.target.value)}
            style={{ background: '#0b0f19', border: '1px solid #334155', color: '#fff', padding: '8px 12px', borderRadius: '6px' }}
          />
        </div>

        <div>
          <label style={{ display: 'block', fontSize: '12px', color: '#94a3b8', marginBottom: '4px' }}>Plan</label>
          <select
            value={planId}
            onChange={(e) => setPlanId(e.target.value)}
            style={{ background: '#0b0f19', border: '1px solid #334155', color: '#fff', padding: '8px 12px', borderRadius: '6px' }}
          >
            <option value="PLAN_FIBRA_300">Fibra 300 Mbps</option>
            <option value="PLAN_FIBRA_500">Fibra 500 Mbps Pro</option>
            <option value="PLAN_GIGA_CORP">Giga Corporativo 1Gbps</option>
          </select>
        </div>

        <div style={{ display: 'flex', gap: '10px', marginLeft: 'auto', flexWrap: 'wrap' }}>
          <button
            disabled={isSimulating}
            onClick={() => triggerFlow('HAPPY_PATH')}
            style={{ background: '#16a34a', color: '#fff', display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <Play size={16} /> Flujo Exitoso
          </button>

          <button
            disabled={isSimulating}
            onClick={() => triggerFlow('BILLING_FAIL')}
            style={{ background: '#d97706', color: '#fff', display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <AlertTriangle size={16} /> Fallo Facturación
          </button>

          <button
            disabled={isSimulating}
            onClick={() => triggerFlow('PROV_FAIL')}
            style={{ background: '#dc2626', color: '#fff', display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <XCircle size={16} /> Fallo Red
          </button>

          <button
            onClick={() => setEvents([])}
            style={{ background: '#334155', color: '#e2e8f0', display: 'flex', alignItems: 'center', gap: '6px' }}
          >
            <RotateCcw size={16} /> Limpiar
          </button>
        </div>
      </div>

      {/* Grid de Monitoreo */}
      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '24px' }}>
        <div>
          <h2 style={{ fontSize: '18px', marginBottom: '14px', display: 'flex', alignItems: 'center', gap: '8px' }}>
            <Clock size={20} color="#38bdf8" /> Stream de Eventos en Topics
          </h2>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            {events.length === 0 ? (
              <div style={{ padding: '40px', textAlign: 'center', background: '#0f172a', borderRadius: '8px', border: '1px dashed #334155', color: '#64748b' }}>
                Sin eventos registrados. Ejecuta uno de los escenarios para observar la secuencia.
              </div>
            ) : (
              events.map((evt) => (
                <div
                  key={evt.id}
                  style={{
                    background: '#0f172a',
                    border: '1px solid #1e293b',
                    borderLeft: `4px solid ${evt.status === 'COMPLETED' ? '#22c55e' : evt.status === 'FAILED' ? '#ef4444' : '#38bdf8'
                      }`,
                    borderRadius: '6px',
                    padding: '12px 14px',
                    display: 'flex',
                    flexDirection: 'column',
                    gap: '6px'
                  }}
                >
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{ fontSize: '12px', background: '#1e293b', padding: '2px 8px', borderRadius: '4px', color: '#94a3b8' }}>
                      Topic: <b>{evt.topic}</b>
                    </span>
                    <span style={{ fontSize: '12px', color: '#64748b' }}>{evt.timestamp}</span>
                  </div>

                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', fontSize: '15px', fontWeight: 'bold' }}>
                    {evt.status === 'COMPLETED' ? <CheckCircle2 size={16} color="#22c55e" /> :
                      evt.status === 'FAILED' ? <XCircle size={16} color="#ef4444" /> :
                        <Clock size={16} color="#38bdf8" />}
                    <span>{evt.eventType}</span>
                    <span style={{ fontSize: '12px', color: '#64748b', fontWeight: 'normal' }}>
                      (CorrId: {evt.correlationId})
                    </span>
                  </div>

                  <pre style={{
                    background: '#070b12',
                    padding: '8px',
                    borderRadius: '4px',
                    fontSize: '12px',
                    color: '#93c5fd',
                    overflowX: 'auto'
                  }}>
                    {JSON.stringify(evt.payload, null, 2)}
                  </pre>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Panel lateral informativo */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          <div style={{ background: '#131c2e', border: '1px solid #1e293b', borderRadius: '8px', padding: '16px' }}>
            <h3 style={{ fontSize: '16px', display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
              <Mail size={18} color="#a855f7" /> Notification Service
            </h3>
            <p style={{ fontSize: '13px', color: '#94a3b8', lineHeight: '1.5' }}>
              Consume <code style={{ color: '#c084fc' }}>activation.events</code>. Envía email vía Mailhog con verificación de idempotencia en MongoDB.
            </p>
            <div style={{ marginTop: '10px' }}>
              <a href="http://localhost:8025" target="_blank" rel="noreferrer" style={{ color: '#38bdf8', fontSize: '13px', textDecoration: 'none' }}>
                &rarr; Ver Mailhog Web (8025)
              </a>
            </div>
          </div>

          <div style={{ background: '#131c2e', border: '1px solid #1e293b', borderRadius: '8px', padding: '16px' }}>
            <h3 style={{ fontSize: '16px', display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
              <Database size={18} color="#10b981" /> CRM Analytics Service
            </h3>
            <p style={{ fontSize: '13px', color: '#94a3b8', lineHeight: '1.5' }}>
              Auditoría desacoplada con <code style={{ color: '#34d399' }}>fromBeginning: true</code> para recrear el log histórico completo.
            </p>
            <div style={{ marginTop: '10px' }}>
              <a href="http://localhost:8080" target="_blank" rel="noreferrer" style={{ color: '#38bdf8', fontSize: '13px', textDecoration: 'none' }}>
                &rarr; Ver Kafka UI (8080)
              </a>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}