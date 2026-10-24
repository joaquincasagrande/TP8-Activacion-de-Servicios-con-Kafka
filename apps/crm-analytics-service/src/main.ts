import { NestFactory } from '@nestjs/core';
import { MicroserviceOptions, Transport } from '@nestjs/microservices';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createMicroservice<MicroserviceOptions>(AppModule, {
    transport: Transport.KAFKA,
    options: {
      client: {
        clientId: 'crm-analytics-svc',
        brokers: [process.env.KAFKA_BROKERS || '127.0.0.1:9092'],
      },
      consumer: {
        groupId: 'crm-analytics-group',
      },
      subscribe: {
        fromBeginning: true,
      },
    },
  });

  await app.listen();
  console.log('--- CRM Analytics Service escuchando eventos en Kafka ---');
}
bootstrap();