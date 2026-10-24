import { Logger, ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { CONSUMER_GROUPS } from '@app/contracts';
import { AppModule } from './app.module';

async function bootstrap() {
  const logger = new Logger('ActivationApiBootstrap');
  const app = await NestFactory.create(AppModule);

  app.enableCors({ origin: '*' });
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      transform: true,
    }),
  );

  const kafkaBrokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(
    ',',
  );

  // Conectar transporte Kafka para el Agregador de la Saga
  app.connectMicroservice<MicroserviceOptions>({
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'activation-api',
        brokers: kafkaBrokers,
        retry: {
          initialRetryTime: 300,
          retries: 10,
        },
      },
      consumer: {
        groupId: CONSUMER_GROUPS.ACTIVATION_API,
        allowAutoTopicCreation: false,
      },
    },
  });

  await app.startAllMicroservices();

  const port = Number(process.env.PORT_ACTIVATION_API ?? process.env.PORT ?? 3000);
  await app.listen(port);
  logger.log(
    `🚀 activation-api escuchando HTTP/WebSocket en http://localhost:${port} y consumiendo Kafka (groupId=${CONSUMER_GROUPS.ACTIVATION_API})`,
  );
}

bootstrap();
