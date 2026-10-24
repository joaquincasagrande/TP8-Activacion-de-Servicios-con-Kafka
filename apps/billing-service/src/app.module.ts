import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { ClientsModule, Transport } from '@nestjs/microservices';
import { MongooseModule } from '@nestjs/mongoose';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';
import {
  BillingAccount,
  BillingAccountSchema,
} from './schemas/billing-account.schema';

const kafkaBrokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');
const mongoUri =
  process.env.MONGO_URI_BILLING ??
  process.env.MONGO_URI ??
  'mongodb://localhost:27017/billing_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: BillingAccount.name, schema: BillingAccountSchema },
    ]),
    ClientsModule.register([
      {
        name: 'KAFKA_PRODUCER',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: 'billing-service-producer',
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
  controllers: [BillingController],
  providers: [BillingService],
})
export class AppModule {}
