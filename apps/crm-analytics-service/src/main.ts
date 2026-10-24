import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { CONSUMER_GROUPS } from '@app/contracts';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('CrmAnalyticsBootstrap');
  const kafkaBrokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(
    ',',
  );
  const groupId = process.env.KAFKA_GROUP_ID ?? CONSUMER_GROUPS.CRM_ANALYTICS;
  const fromBeginning = process.env.KAFKA_FROM_BEGINNING !== 'false';

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: `crm-analytics-${groupId}`,
          brokers: kafkaBrokers,
          retry: {
            initialRetryTime: 300,
            retries: 10,
          },
        },
        consumer: {
          groupId,
          allowAutoTopicCreation: false,
        },
        subscribe: {
          fromBeginning,
        },
      },
    },
  );

  await app.listen();
  logger.log(
    `📊 crm-analytics-service activo y escuchando los 4 tópicos (groupId=${groupId}, fromBeginning=${fromBeginning})`,
  );
}

bootstrap();
