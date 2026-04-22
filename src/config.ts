import dotenv from 'dotenv';

dotenv.config();

const requiredVars = ['KAFKA_BROKERS', 'KAFKA_CLIENT_ID', 'KAFKA_GROUP_ID', 'TOPIC_NAME'] as const;

// for (const envVar of requiredVars) {
//   if (!process.env[envVar]) {
//     throw new Error(`Missing required environment variable: ${envVar}`);
//   }
// }

export const config = {
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? '').split(',').map((broker) => broker.trim()).filter(Boolean),
  kafkaClientId: process.env.KAFKA_CLIENT_ID ?? '',
  kafkaGroupId: process.env.KAFKA_GROUP_ID ?? '',
  topicName: process.env.TOPIC_NAME ?? '',
  port: Number(process.env.PORT ?? 3000)
};
