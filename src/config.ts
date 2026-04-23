import dotenv from "dotenv";

dotenv.config();

export const config = {
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? "")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean),
  kafkaClientId: process.env.KAFKA_CLIENT_ID ?? "",
  kafkaGroupId: process.env.KAFKA_GROUP_ID ?? "",
  topicName: process.env.TOPIC_NAME ?? "",
  schemaRegistryUrl: process.env.SCHEMA_REGISTRY_URL ?? "",
  port: Number(process.env.PORT ?? 3000),
};
