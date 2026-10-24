import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { MongooseModule } from '@nestjs/mongoose';
import { NotificationController } from './notification.controller';
import { NotificationService } from './notification.service';
import {
  NotificationRecord,
  NotificationRecordSchema,
} from './schemas/notification.schema';

const mongoUri =
  process.env.MONGO_URI_NOTIFICATION ??
  process.env.MONGO_URI ??
  'mongodb://localhost:27017/notification_db?replicaSet=rs0&directConnection=true';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    MongooseModule.forRoot(mongoUri),
    MongooseModule.forFeature([
      { name: NotificationRecord.name, schema: NotificationRecordSchema },
    ]),
  ],
  controllers: [NotificationController],
  providers: [NotificationService],
})
export class AppModule {}
