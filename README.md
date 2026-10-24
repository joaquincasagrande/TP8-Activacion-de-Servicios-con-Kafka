# POC: Activación de Servicios con Kafka, NestJS y React

> **Materia:** Sistemas Distribuidos (4° Año - UTN FRCU)  
> **Patrón:** Saga Coreografiada con Agregador  
> **Stack:** Apache Kafka (KRaft) · NestJS · MongoDB (Replica Set) · Socket.IO · React (Vite) · Mailhog

Esta Prueba de Concepto (POC) demuestra la activación de planes y servicios de telecomunicaciones mediante una **arquitectura orientada a eventos sobre Apache Kafka**. Ningún microservicio se comunica directamente con otro mediante HTTP; toda la coordinación, coreografía y compensación ante fallos se resuelve mediante eventos.

---

## Arquitectura de Microservicios

1. **`apps/activation-api`** (Puerto `3000`):
   - Expone la API REST (`POST /activations` con HTTP 202 Accepted y `GET /activations/:id`).
   - Actúa como **Agregador de la Saga** consumiendo `billing.events` y `provisioning.events`.
   - Transiciona los estados (`PENDING` $\rightarrow$ `IN_PROGRESS` $\rightarrow$ `ACTIVE` / `FAILED`).
   - Emite actualizaciones en tiempo real a la UI vía **WebSocket (Socket.IO)**.
2. **`apps/billing-service`** (Worker Kafka):
   - Consume `activation.requested` y crea la cuenta de facturación (`BillingAccountCreated`).
   - Escucha `activation.events`: si recibe `ActivationFailed`, ejecuta la **compensación de la Saga** anulando la cuenta (`BillingAccountCancelled`).
3. **`apps/provisioning-service`** (Worker Kafka):
   - Consume `activation.requested` en paralelo con retardo simulado de 1 a 3 segundos.
   - Publica `ProvisioningCompleted` o simula fallo técnico inyectado (`ProvisioningFailed`).
4. **`apps/notification-service`** (Worker Kafka):
   - Consume `activation.events` y despacha emails HTML transaccionales (bienvenida o aviso de fallo) vía SMTP a Mailhog.
   - Implementa verificación de **idempotencia** para evitar emails duplicados.
5. **`apps/crm-analytics-service`** (Worker Kafka):
   - Consumidor pasivo suscrito a los 4 tópicos para auditoría histórica de punta a punta (`event_log`).
6. **`apps/demo-ui`** (Puerto `5173`):
   - Consola visual interactiva en React + Vite + Lucide Icons conectada en vivo por WebSocket a `activation-api`.

---

## Tópicos de Kafka (3 particiones, clave = `customerId`)

- `activation.requested` (+ `.dlq`)
- `billing.events` (+ `.dlq`)
- `provisioning.events` (+ `.dlq`)
- `activation.events` (+ `.dlq`)

---

## Cómo Ejecutar el Proyecto

### 1. Requisitos Previos
- Docker y Docker Compose
- Node.js (v18+)
- pnpm (`npm install -g pnpm`)

### 2. Levantar la Infraestructura
```bash
docker compose up -d
```
*(Inicia Kafka en modo KRaft, MongoDB Replica Set `rs0`, Kafka UI y Mailhog).*

### 3. Instalar Dependencias del Monorepo
```bash
pnpm install
```

### 4. Iniciar Microservicios y Frontend
```bash
pnpm dev:all
```
*(Levanta en simultáneo los 5 microservicios NestJS y la UI de React).*

---

## Paneles y Accesos Rápidos

| Componente | URL | Descripción |
| :--- | :--- | :--- |
| **Consola Demo UI** | [http://localhost:5173](http://localhost:5173) | Interfaz web interactiva con timeline WebSocket en vivo |
| **API REST / WS** | [http://localhost:3000](http://localhost:3000) | Endpoint base de activaciones |
| **Kafka UI (Kafbat)** | [http://localhost:8080](http://localhost:8080) | Inspección de tópicos, particiones, offsets y mensajes |
| **Mailhog (Web)** | [http://localhost:8025](http://localhost:8025) | Bandeja de entrada de correos enviados por notificación |
