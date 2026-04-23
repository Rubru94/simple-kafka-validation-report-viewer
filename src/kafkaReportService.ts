import { Kafka, logLevel } from "kafkajs";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { config } from "./config";

type KafkaValidationResult = {
  validTrue: number;
  validFalse: number;
  processedMessages: number;
  startOffset: string;
  latestOffsetExclusive: string;
};

type ValidationMessage = {
  taskRevisionId: string;
  externalId: string;
  jiraId: string;
  id: string;
  statusCode: string;
  statusText: string;
  valid: boolean;
  brokenRules: string[];
  validationTokens: number;
  validationModel: string;
  validationCost: number;
  validationDurationTime: number;
};

const kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBrokers,
  logLevel: logLevel.NOTHING,
});

const registry = new SchemaRegistry({
  host: config.schemaRegistryUrl,
});

export async function buildValidationReport(
  startOffsetInput: string,
): Promise<KafkaValidationResult> {
  const startOffset = normalizeOffset(startOffsetInput);
  const admin = kafka.admin();

  await admin.connect();

  try {
    const topicOffsets = await admin.fetchTopicOffsets(config.topicName);

    if (topicOffsets.length === 0) {
      return {
        validTrue: 0,
        validFalse: 0,
        processedMessages: 0,
        startOffset,
        latestOffsetExclusive: startOffset,
      };
    }

    const latestOffsetExclusive = getMaxOffset(
      topicOffsets.map((offsetInfo) => offsetInfo.offset),
    );

    const partitionsPending = new Set<number>();
    const endOffsetsByPartition = new Map<number, bigint>();

    for (const partitionOffset of topicOffsets) {
      const partition = partitionOffset.partition;
      const endOffsetExclusive = BigInt(partitionOffset.offset);
      endOffsetsByPartition.set(partition, endOffsetExclusive);

      if (BigInt(startOffset) < endOffsetExclusive) {
        partitionsPending.add(partition);
      }
    }

    if (partitionsPending.size === 0) {
      return {
        validTrue: 0,
        validFalse: 0,
        processedMessages: 0,
        startOffset,
        latestOffsetExclusive,
      };
    }

    const consumer = kafka.consumer({
      groupId: `${config.kafkaGroupId}-${Date.now()}-${Math.floor(Math.random() * 10000)}`,
    });

    let validTrue = 0;
    let validFalse = 0;
    let processedMessages = 0;

    await consumer.connect();

    try {
      await consumer.subscribe({
        topic: config.topicName,
        fromBeginning: true,
      });

      let resolveFinished: (() => void) | null = null;
      const processingFinished = new Promise<void>((resolve) => {
        resolveFinished = resolve;
      });

      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ message, partition, heartbeat }) => {
          const currentOffset = BigInt(message.offset);
          const rangeStart = BigInt(startOffset);
          const rangeEndExclusive = endOffsetsByPartition.get(partition);

          if (!rangeEndExclusive) return;
          if (currentOffset < rangeStart) return;
          if (currentOffset >= rangeEndExclusive) return;

          const parsed = await parseKafkaMessage(message.value);

          processedMessages += 1;

          if (parsed?.valid === true) {
            validTrue += 1;
          } else if (parsed?.valid === false) {
            validFalse += 1;
          }

          const lastOffsetInPartition = rangeEndExclusive - 1n;
          if (currentOffset >= lastOffsetInPartition) {
            partitionsPending.delete(partition);

            if (partitionsPending.size === 0) {
              resolveFinished?.();
            }
          }

          await heartbeat();
        },
      });

      for (const partitionOffset of topicOffsets) {
        if (BigInt(startOffset) < BigInt(partitionOffset.offset)) {
          consumer.seek({
            topic: config.topicName,
            partition: partitionOffset.partition,
            offset: startOffset,
          });
        }
      }

      await Promise.race([processingFinished, waitForCatchUp(30_000)]);
    } finally {
      await consumer.stop();
      await consumer.disconnect();
    }

    return {
      validTrue,
      validFalse,
      processedMessages,
      startOffset,
      latestOffsetExclusive,
    };
  } finally {
    await admin.disconnect();
  }
}

async function parseKafkaMessage(
  value: Buffer | null,
): Promise<ValidationMessage | null> {
  if (!value) return null;

  try {
    const decoded = await registry.decode(value);

    if (
      typeof decoded === "object" &&
      decoded !== null &&
      typeof (decoded as any).valid === "boolean"
    ) {
      return decoded as ValidationMessage;
    }

    return null;
  } catch (err) {
    console.error("Schema decode error:", err);
    return null;
  }
}

function normalizeOffset(input: string): string {
  const trimmed = input.trim();

  if (!/^\d+$/.test(trimmed)) {
    throw new Error("Starting offset must be a non-negative integer.");
  }

  return trimmed;
}

async function waitForCatchUp(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

function getMaxOffset(offsets: string[]): string {
  let currentMax = 0n;

  for (const offset of offsets) {
    const asBigInt = BigInt(offset);
    if (asBigInt > currentMax) {
      currentMax = asBigInt;
    }
  }

  return currentMax.toString();
}

export type { KafkaValidationResult };
