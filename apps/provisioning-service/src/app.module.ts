import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { MongooseModule } from '@nestjs/mongoose';
import { ProvisioningController } from './provisioning.controller';
import { ProvisioningService } from './provisioning.service';
import {
  ProvisioningOrder,
  ProvisioningOrderSchema,
} from './schemas/provisioning-order.schema';

const kafkaBrokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const mongoUri =
  process.env.MONGO_URI_PROVISIONING ??
  process.env.MONGO_URI ??
  'mongodb://localhost:27017/provisioning_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: ProvisioningOrder.name, schema: ProvisioningOrderSchema },
    ]),
    ClientsModule.register([
      {
        name: 'KAFKA_PRODUCER',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'provisioning-service-producer',
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
  controllers: [ProvisioningController],
  providers: [ProvisioningService],
})
export class AppModule {}
