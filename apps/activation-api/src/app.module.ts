import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { MongooseModule } from '@nestjs/mongoose';
import { CONSUMER_GROUPS } from '@app/contracts';
import { ActivationsController } from './activations.controller';
import { ActivationsService } from './activations.service';
import { ActivationsGateway } from './activations.gateway';
import { Activation, ActivationSchema } from './schemas/activation.schema';

const kafkaBrokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const mongoUri =
  process.env.MONGO_URI_ACTIVATION ??
  process.env.MONGO_URI ??
  'mongodb://localhost:27017/activation_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: Activation.name, schema: ActivationSchema },
    ]),
    ClientsModule.register([
      {
        name: 'KAFKA_PRODUCER',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'activation-api-producer',
            brokers: kafkaBrokers,
            retry: {
              initialRetryTime: 300,
              retries: 10,
            },
          },
          producerOnlyMode: true,
        },
      },
    ]),
  ],
  controllers: [ActivationsController],
  providers: [ActivationsService, ActivationsGateway],
})
export class AppModule {}
