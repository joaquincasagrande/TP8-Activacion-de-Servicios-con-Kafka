# Guía de Pruebas y Arquitectura - Integrante 2

## 1. Servicios Implementados
* **`notification-service`**: Microservicio NestJS consumidor del topic `activation.events`. Despacha correos mediante SMTP hacia Mailhog y aplica control estricto de idempotencia en MongoDB mediante la colección `processed_events` con índice único en `eventId`.
* **`crm-analytics-service`**: Consumidor desacoplado con replay histórico activado (`fromBeginning: true`). Persiste todos los eventos de los topics (`activation.requested`, `billing.events`, `provisioning.events`, `activation.events`) en la colección `event_log`.
* **`demo-ui`**: Interfaz interactiva en React + Vite para visualizar la línea de tiempo, simular fallos de negocio (facturación, red) y monitorear la orquestación distribuida.

---

## 2. Puntos de Acceso e Infraestructura
* **Kafka UI:** `http://localhost:8080` (Monitoreo de brokers, topics y grupos de consumo)
* **Mailhog UI:** `http://localhost:8025` (Bandeja de correos simulados)
* **Demo UI:** `http://localhost:5173` (Consola de simulación de escenarios)
* **MongoDB:** `localhost:27017` (Replica Set `rs0`)

---

## 3. Guion de Demostración para la Presentación

### Prueba A: Flujo Feliz y Envío de Correo
1. Acceder a **Kafka UI** (`http://localhost:8080`) e ir al topic `activation.events`.
2. Publicar un mensaje con `eventType: "ActivationCompleted"` y un nuevo `eventId` (`evt-100`).
3. Verificar en la consola de `notification-service` el log `[Email enviado]`.
4. Abrir **Mailhog** (`http://localhost:8025`) y constatar la recepción del correo de bienvenida.

### Prueba B: Demostración de Idempotencia (Tolerancia a Reintentos)
1. En **Kafka UI**, volver a emitir exactamente el mismo mensaje (`evt-100`).
2. Constatar en la consola: `[Idempotencia] Evento duplicado ignorado: evt-100`.
3. Verificar en **Mailhog** que **no** se duplicó el envío del correo.

### Prueba C: Log Replay y Auditoría en CRM
1. Detener el microservicio `crm-analytics-service`.
2. Emitir varios eventos en Kafka.
3. Volver a iniciar el servicio: se comprobará que procesa todos los eventos anteriores en orden gracias a `fromBeginning: true`, registrándolos en la colección `event_log`.

### Prueba D: Simulación de Fallos desde Demo UI
1. Ingresar a `http://localhost:5173`.
2. Ejecutar "Fallo Facturación" o "Fallo Red" para visualizar el corte de la coreografía y el despacho del evento `ActivationFailed`.