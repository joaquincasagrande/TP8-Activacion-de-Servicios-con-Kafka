import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { CrmController } from './crm.controller';
import { CrmService } from './crm.service';
import { EventLog, EventLogSchema } from './schemas/event-log.schema';

const mongoUri =
  process.env.MONGO_URI_CRM ??
  process.env.MONGO_URI ??
  'mongodb://localhost:27017/crm_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: EventLog.name, schema: EventLogSchema },
    ]),
  ],
  controllers: [CrmController],
  providers: [CrmService],
})
export class AppModule {}
