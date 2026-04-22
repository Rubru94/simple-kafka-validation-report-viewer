import { Kafka, logLevel } from "kafkajs";
import { config } from "./config";

type KafkaValidationResult = {
  validTrue: number;
  validFalse: number;
  processedMessages: number;
  startOffset: string;
  latestOffsetExclusive: string;
};

const kafka = new Kafka({
  clientId: config.kafkaClientId,
  brokers: config.kafkaBrokers,
  logLevel: logLevel.NOTHING,
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

          if (!rangeEndExclusive) {
            return;
          }

          if (currentOffset < rangeStart) {
            return;
          }

          if (currentOffset >= rangeEndExclusive) {
            return;
          }

          processedMessages += 1;
          classifyMessage(message.value, (isValid) => {
            if (isValid === true) {
              validTrue += 1;
            } else if (isValid === false) {
              validFalse += 1;
            }
          });

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

function normalizeOffset(input: string): string {
  const trimmed = input.trim();

  if (!/^\d+$/.test(trimmed)) {
    throw new Error("Starting offset must be a non-negative integer.");
  }

  return trimmed;
}

function classifyMessage(
  value: Buffer | null,
  onClassified: (result: boolean | null) => void,
): void {
  if (!value) {
    onClassified(null);
    return;
  }

  try {
    const parsed: unknown = JSON.parse(value.toString("utf-8"));

    if (typeof parsed === "object" && parsed !== null && "valid" in parsed) {
      const validValue = (parsed as { valid: unknown }).valid;
      if (validValue === true) {
        onClassified(true);
        return;
      }
      if (validValue === false) {
        onClassified(false);
        return;
      }
    }

    onClassified(null);
  } catch {
    onClassified(null);
  }
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
