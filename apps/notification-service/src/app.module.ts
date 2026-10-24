import { Module } from '@nestjs/common';
import { MongooseModule } from '@nestjs/mongoose';
import { AppController } from './app.controller';
import {
  NotificationLog,
  NotificationSchema,
  ProcessedEvent,
  ProcessedEventSchema,
} from './schemas';

@Module({
  imports: [
    MongooseModule.forRoot(
      process.env.MONGO_URI || 'mongodb://localhost:27017/notification_db?directConnection=true',
    ),
    MongooseModule.forFeature([
      { name: NotificationLog.name, schema: NotificationSchema },
      { name: ProcessedEvent.name, schema: ProcessedEventSchema },
    ]),
  ],
  controllers: [AppController],
})
export class AppModule { }