import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { CONSUMER_GROUPS } from '@app/contracts';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('NotificationServiceBootstrap');
  const kafkaBrokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(
    ',',
  );

  const app = await NestFactory.createMicroservice<MicroserviceOptions>(
    AppModule,
    {
      transport: Transport.KAFKA,
      options: {
        client: {
          clientId: 'notification-service',
          brokers: kafkaBrokers,
          retry: {
            initialRetryTime: 300,
            retries: 10,
          },
        },
        consumer: {
          groupId: CONSUMER_GROUPS.NOTIFICATION_SVC,
          allowAutoTopicCreation: false,
        },
      },
    },
  );

  await app.listen();
  logger.log(
    `📧 notification-service activo y consumiendo Kafka (groupId=${CONSUMER_GROUPS.NOTIFICATION_SVC})`,
  );
}

bootstrap();
