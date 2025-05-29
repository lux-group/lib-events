import {
  Message,
  MessageAttributes,
  MessageHandler,
  QueueClient,
} from "./queueClient";
import {
  Message as SqsMessage,
  ReceiveMessageCommand,
  SQS,
} from "@aws-sdk/client-sqs";
import { MessageAttributeValue } from "@aws-sdk/client-sqs/dist-types/models/models_0";
import { createContextualLogger } from "@luxuryescapes/lib-logger";
import { Logger } from "@luxuryescapes/lib-logger/lib/types";

const dataTypeMap: Record<string, string> = {
  string: "String",
  number: "Number",
  object: "Binary",
};

const DEFAULT_LONG_POLL_DURATION_SECONDS = 1;
const DEFAULT_MAX_NUMBER_OF_MESSAGES = 10;
const DEFAULT_VISIBILITY_TIMEOUT_SECONDS = 30;

export class SqsQueueClient implements QueueClient {
  private readonly client: SQS;
  private readonly queueUrl: string;
  private readonly longPollDurationSeconds: number;
  private readonly maxNumberOfMessages: number;
  private readonly visibilityTimeoutSeconds: number;
  private readonly logger: Logger;

  private readonly messageHandlers: Map<string, MessageHandler<unknown>>;

  constructor(
    queueUrl: string,
    {
      longPollDurationSeconds,
      maxNumberOfMessages,
      visibilityTimeoutSeconds,
      region,
      accessKeyId,
      secretAccessKey,
      logger,
    }: {
      longPollDurationSeconds?: number;
      maxNumberOfMessages?: number;
      visibilityTimeoutSeconds?: number;
      accessKeyId?: string;
      region?: string;
      secretAccessKey?: string;
      logger?: Logger;
    }
  ) {
    if (region && accessKeyId && secretAccessKey) {
      this.client = new SQS({
        region,
        endpoint: queueUrl,
        credentials: {
          accessKeyId,
          secretAccessKey,
        },
      });
    } else {
      this.client = new SQS({ endpoint: queueUrl });
    }

    this.queueUrl = queueUrl;
    this.longPollDurationSeconds =
      longPollDurationSeconds ?? DEFAULT_LONG_POLL_DURATION_SECONDS;
    this.maxNumberOfMessages =
      maxNumberOfMessages ?? DEFAULT_MAX_NUMBER_OF_MESSAGES;
    this.visibilityTimeoutSeconds =
      visibilityTimeoutSeconds ?? DEFAULT_VISIBILITY_TIMEOUT_SECONDS;
    this.messageHandlers = new Map();
    this.logger = (
      logger ??
      createContextualLogger({
        logLevel: "info",
        service: "Sqs client",
        env: process.env.APP_ENV ?? "spec",
      })
    ).child({ queueUrl });
  }

  async health(): Promise<boolean> {
    try {
      const response = await this.client.getQueueAttributes({
        QueueUrl: this.queueUrl,
        AttributeNames: ["QueueArn"],
      });
      return !!response.Attributes?.QueueArn;
    } catch (error) {
      this.logger.error("SQS health check failed:", error);
      return Promise.reject(
        new Error(`SQS health check failed: ${JSON.stringify(error)}`)
      );
    }
  }

  registerMessageHandler<
    Body,
    Attributes extends MessageAttributes = MessageAttributes
  >(handler: MessageHandler<Body, Attributes>) {
    if (this.messageHandlers.has(handler.type)) {
      throw new Error(
        `Message handler for type ${handler.type} already exists`
      );
    }

    this.messageHandlers.set(
      handler.type,
      handler as unknown as MessageHandler<unknown>
    );
  }

  /**
   * Sends messages to the SQS queue. The total number of messages in a single
   * batch cannot exceed 10. The total size of a message batch cannot exceed 256 kb.
   */
  async sendMessages(
    ...messages: (Message<unknown> & {
      delaySeconds?: number;
    })[]
  ): Promise<void> {
    if (messages.length > 10) {
      throw new Error("Cannot send more than 10 messages in a batch.");
    }

    const result = await this.client.sendMessageBatch({
      QueueUrl: this.queueUrl,
      Entries: messages.map((message, index) => ({
        Id: index.toString(),
        DelaySeconds: message.delaySeconds ?? 0,
        MessageBody: JSON.stringify(message.body),
        MessageGroupId: message.messageGroupId?.toString(),
        MessageDeduplicationId: message.messageDeduplicationId?.toString(),
        MessageAttributes: {
          ...this.mapAttributesToSqsMessageAttributes(message.attributes),
          ...this.formatMessageType(message.type),
        },
      })),
    });
    if (result.Failed?.length) {
      throw new Error(
        `Failed to send messages: ${JSON.stringify(result.Failed)}`
      );
    }
  }

  async startPollForMessages(): Promise<never> {
    // eslint-disable-next-line no-constant-condition
    while (true) {
      await this.pollOnceForMessages();
    }
  }

  /**
   * This method polls once for messages and processes them.
   */
  async pollOnceForMessages(): Promise<void> {
    try {
      const command = new ReceiveMessageCommand({
        QueueUrl: this.queueUrl,
        MaxNumberOfMessages: this.maxNumberOfMessages,
        WaitTimeSeconds: this.longPollDurationSeconds,
        VisibilityTimeout: this.visibilityTimeoutSeconds,
        MessageAttributeNames: ["All"],
      });
      const receivedMessages = await this.client.send(command);

      const messageHandlerPromises =
        receivedMessages.Messages?.map(async (message) => {
          try {
            const mappedMessage = this.mapSqsMessageToInternalMessage(message);
            const { handler } = await this.handleMessage(mappedMessage);
            await this.deleteSqsMessage(message, handler.type);
          } catch (error) {
            this.logger.error("Error processing message. Skipping delete.", {
              error: {
                message: (error as Error).message,
                stack: (error as Error).stack,
              },
              sqsMessage: message,
            });
          }
        }) ?? [];

      await Promise.all(messageHandlerPromises);
    } catch (error) {
      this.logger.error("Error polling messages. Skipping and retrying.", {
        error: {
          message: (error as Error).message,
          stack: (error as Error).stack,
        },
      });
    }
  }

  private formatMessageType(
    messageType: string
  ): Record<string, MessageAttributeValue> {
    return messageType
      ? {
          type: {
            StringValue: messageType,
            DataType: "String",
          },
        }
      : {};
  }

  private deleteSqsMessage(
    message: SqsMessage,
    messageType: string
  ): Promise<void> {
    const contextAwareLogger = this.logger.child({ messageType });
    contextAwareLogger.info("Deleting message from SQS");

    return message.ReceiptHandle
      ? this.client
          .deleteMessage({
            QueueUrl: this.queueUrl,
            ReceiptHandle: message.ReceiptHandle,
          })
          .then(() => {
            contextAwareLogger.info("Deleted message from SQS");
          })
          .catch((error) => {
            contextAwareLogger.error("Error deleting message from SQS", {
              error: {
                message: (error as Error).message,
                stack: (error as Error).stack,
              },
            });
          })
      : Promise.resolve();
  }

  private async handleMessage<
    Body,
    Attributes extends MessageAttributes = MessageAttributes
  >(
    message: Message<Body, Attributes>
  ): Promise<{
    message: Message<Body, Attributes>;
    handler: MessageHandler<Body, Attributes>;
  }> {
    // Find the message handler
    const handler = this.messageHandlers.get(message.type) as
      | MessageHandler<Body, Attributes>
      | undefined;
    if (!handler) {
      throw new Error(`No handler found for message type ${message.type}`);
    }

    // Validate the message
    if (!handler.validateMessage(message)) {
      throw new Error(`Invalid message: ${JSON.stringify(message)}`);
    }

    this.logger.info(`Processing message from queue`, {
      handler: handler.type,
      sqsMessage: message,
    });

    // Process the message
    await handler.handleMessage(message);
    return { message, handler };
  }

  private mapAttributesToSqsMessageAttributes(
    attributes: MessageAttributes
  ): Record<string, MessageAttributeValue> {
    return Object.entries(attributes)
      .map(
        ([key, value]) =>
          ({
            [key]: {
              DataType: dataTypeMap[typeof value],
              ...(dataTypeMap[typeof value] === "Binary"
                ? {
                    BinaryValue: new TextEncoder().encode(
                      JSON.stringify(value)
                    ),
                  }
                : // eslint-disable-next-line @typescript-eslint/no-base-to-string
                  { StringValue: value.toString() }),
            },
          } as Record<string, MessageAttributeValue>)
      )
      .reduce((acc, curr) => ({ ...acc, ...curr }), {});
  }

  private mapSqsMessageToInternalMessage(
    message: SqsMessage
  ): Message<unknown> {
    const messageBody = JSON.parse(message.Body ?? "{}") as Record<
      string,
      unknown
    >;

    // Checking if the message came from an SNS notification
    if (
      !message.MessageAttributes?.Type &&
      messageBody?.Type === "Notification"
    ) {
      const snsMessage = messageBody as {
        Message: string;
        MessageAttributes: Record<string, { Type: string; Value: unknown }>;
      };
      return {
        type:
          (snsMessage.MessageAttributes?.type?.Value as string) ?? "UNKNOWN",
        attributes: Object.entries(snsMessage.MessageAttributes ?? {})
          .map(([key, value]) => this.mapSnsAttributeToRecord(key, value))
          .reduce((acc, curr) => ({ ...acc, ...curr }), {}),
        body: JSON.parse(snsMessage.Message ?? "{}"),
      };
    }

    return {
      type: message.MessageAttributes?.type?.StringValue ?? "UNKNOWN",
      attributes: Object.entries(message.MessageAttributes ?? {})
        .map(([key, value]) => this.mapSqsAttributeToRecord(key, value))
        .reduce((acc, curr) => ({ ...acc, ...curr }), {}),
      body: JSON.parse(message.Body ?? "{}"),
    };
  }

  private mapSqsAttributeToRecord(
    key: string,
    value: MessageAttributeValue
  ): MessageAttributes {
    let resolvedValue: string | number | object;
    if (value.DataType === "String") {
      resolvedValue = value.StringValue ?? "";
    } else if (value.DataType === "Number") {
      resolvedValue = Number(value.StringValue);
    } else if (value.DataType === "Binary") {
      resolvedValue = JSON.parse(
        new TextDecoder().decode(value.BinaryValue)
      ) as object;
    } else {
      throw new Error(`Unsupported data type: ${value.DataType}`);
    }

    return { [key]: resolvedValue };
  }

  private mapSnsAttributeToRecord(
    key: string,
    value: { Type: string; Value: unknown }
  ): MessageAttributes {
    let resolvedValue;
    if (value.Type === "String") {
      resolvedValue = value.Value as string;
    } else if (value.Type === "Number") {
      resolvedValue = Number(value.Value);
    } else if (value.Type === "Binary") {
      resolvedValue = {}; // TODO: Deal with this
    } else {
      throw new Error(`Unsupported data type: ${JSON.stringify(value)}`);
    }

    return { [key]: resolvedValue };
  }
}
