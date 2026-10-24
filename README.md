# POC: Activación de Servicios con Kafka y NestJS

Esta Prueba de Concepto (POC) demuestra la orquestación descentralizada (coreografía) de la activación de un servicio utilizando una arquitectura orientada a eventos. Ningún microservicio se comunica directamente con otro mediante HTTP; toda la coordinación se realiza a través de Apache Kafka.

## Arquitectura

El sistema está compuesto por una UI en React y 5 microservicios backend desarrollados en NestJS:
- **activation-api:** Punto de entrada REST, agregador del patrón Saga y gestor de estado.
- **billing-service:** Simula la creación y anulación de cuentas de facturación (Compensación).
- **provisioning-service:** Simula el aprovisionamiento de líneas con retardo.
- **notification-service:** Escucha el resultado final y envía correos vía Mailhog.
- **crm-analytics-service:** Consumidor pasivo que registra el historial (Log/Replay).

## Requisitos Previos
- Docker y Docker Compose
- Node.js (v18+)
- pnpm (Recomendado para manejar el monorepo)

## Stack Tecnológico
- **Backend:** NestJS, `@nestjs/microservices` (Kafka Transport)
- **Mensajería:** Apache Kafka (Modo KRaft)
- **Base de Datos:** MongoDB (Replica Set para transacciones/Outbox)
- **Herramientas de Observabilidad:** Kafbat (Kafka UI), Mailhog
