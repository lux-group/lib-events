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

  private readonly messageHandlers: Map<
    string,
    MessageHandler<MessageAttributes, unknown>
  >;

  constructor(
    queueUrl: string,
    longPollDurationSeconds: number = DEFAULT_LONG_POLL_DURATION_SECONDS,
    maxNumberOfMessages: number = DEFAULT_MAX_NUMBER_OF_MESSAGES,
    visibilityTimeoutSeconds: number = DEFAULT_VISIBILITY_TIMEOUT_SECONDS,
    region?: string,
    accessKeyId?: string,
    secretAccessKey?: string
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
    this.longPollDurationSeconds = longPollDurationSeconds;
    this.maxNumberOfMessages = maxNumberOfMessages;
    this.visibilityTimeoutSeconds = visibilityTimeoutSeconds;
    this.messageHandlers = new Map();
  }

  async health(): Promise<boolean> {
    try {
      const response = await this.client.getQueueAttributes({
        QueueUrl: this.queueUrl,
        AttributeNames: ["QueueArn"],
      });
      return !!response.Attributes?.QueueArn;
    } catch (error) {
      console.error("SQS health check failed:", error);
      return Promise.reject(`SQS health check failed: ${error}`);
    }
  }

  registerMessageHandler<Attributes extends MessageAttributes, Body>(
    handler: MessageHandler<Attributes, Body>
  ) {
    if (this.messageHandlers.has(handler.type)) {
      throw new Error(
        `Message handler for type ${handler.type} already exists`
      );
    }

    this.messageHandlers.set(
      handler.type,
      handler as unknown as MessageHandler<MessageAttributes, unknown>
    );
  }

  async sendMessages(
    ...messages: (Message<MessageAttributes, unknown> & {
      delaySeconds?: number;
    })[]
  ): Promise<void> {
    const result = await this.client.sendMessageBatch({
      QueueUrl: this.queueUrl,
      Entries: messages.map((message, index) => ({
        Id: index.toString(),
        DelaySeconds: message.delaySeconds ?? 0,
        MessageBody: JSON.stringify(message.body),
        MessageAttributes: {
          ...this.mapAttributesToSqsMessageAttributes(message.attributes),
          type: {
            StringValue: message.type,
            DataType: "String",
          },
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

      // Small delay to prevent hot-looping
      await new Promise((resolve) =>
        setTimeout(resolve, this.longPollDurationSeconds * 1000)
      );
    }
  }

  private deleteSqsMessage(message: SqsMessage): Promise<void> {
    return message.ReceiptHandle
      ? this.client
          .deleteMessage({
            QueueUrl: this.queueUrl,
            ReceiptHandle: message.ReceiptHandle,
          })
          .then(() => {
            return;
          })
      : Promise.resolve();
  }

  private handleMessage<Attributes extends MessageAttributes, Body>(
    message: Message<Attributes, Body>
  ): Promise<void> {
    // Find the message handler
    const handler = this.messageHandlers.get(message.type);
    if (!handler) {
      throw new Error(`No handler found for message type ${message.type}`);
    }

    // Validate the message
    if (!handler.validateMessage(message)) {
      throw new Error(`Invalid message: ${JSON.stringify(message)}`);
    }

    // Process the message
    return handler.handleMessage(message);
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
                : { StringValue: value.toString() }),
            },
          } as Record<string, MessageAttributeValue>)
      )
      .reduce((acc, curr) => ({ ...acc, ...curr }), {});
  }

  private mapSqsMessageToInternalMessage(
    message: SqsMessage
  ): Promise<Message<MessageAttributes, unknown>> {
    // TODO: Handle SNS messages where the message attributes and body are
    // wrapped in another object that is shoved into the SQS body
    return Promise.resolve({
      type: message.MessageAttributes?.type?.StringValue ?? "UNKNOWN",
      attributes: Object.entries(message.MessageAttributes ?? {})
        .map(([key, value]) => this.mapSqsAttributeToRecord(key, value))
        .reduce((acc, curr) => ({ ...acc, ...curr }), {}) as MessageAttributes,
      body: JSON.parse(message.Body ?? "{}"),
    });
  }

  private mapSqsAttributeToRecord(
    key: string,
    value: MessageAttributeValue
  ): MessageAttributes {
    let resolvedValue;
    if (value.DataType === "String") {
      resolvedValue = value.StringValue;
    } else if (value.DataType === "Number") {
      resolvedValue = Number(value.StringValue);
    } else if (value.DataType === "Binary") {
      resolvedValue = JSON.parse(new TextDecoder().decode(value.BinaryValue));
    } else {
      throw new Error(`Unsupported data type: ${value.DataType}`);
    }

    return { [key]: resolvedValue };
  }

  /**
   * This method polls once for messages and processes then.
   * @private This message can be used in tests to avoid needing to work with
   * the infinite loop.
   */
  private async pollOnceForMessages(): Promise<void> {
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
        receivedMessages.Messages?.map((message) =>
          this.mapSqsMessageToInternalMessage(message)
            .then((message) => this.handleMessage(message))
            .then(() => this.deleteSqsMessage(message))
            .catch(() => {
              console.error("Error processing message. Skipping delete.", {});
            })
        ) ?? [];

      await Promise.all(messageHandlerPromises);
    } catch (error) {
      console.error(
        "Error polling/processing messages. Skipping and retrying.",
        {
          error: {
            message: (error as Error).message,
            stack: (error as Error).stack,
          },
        }
      );
    }
  }
}
