import { GenericContainer, StartedTestContainer } from "testcontainers";
import { CreateQueueCommand, SQS } from "@aws-sdk/client-sqs";
import { SqsQueueClient } from "./sqsQueueClient";
import { MessageAttributes, MessageHandler } from "./queueClient";

describe("SqsQueueClient", () => {
  jest.setTimeout(30_000);

  let container: StartedTestContainer;
  let queueUrl: string;
  let fifoUrl: string;

  let testSqsClient: SQS;
  let testFifoClient: SQS;

  let sqsQueueClient: SqsQueueClient;
  let fifoQueueClient: SqsQueueClient;

  beforeAll(async () => {
    queueUrl = process.env.SQS_QUEUE_URL || "";

    if (!queueUrl) {
      ({ queueUrl, fifoUrl } = await startElasticMq());
    }

    testSqsClient = new SQS({
      region: "ap-southeast-2",
      endpoint: queueUrl,
      credentials: {
        accessKeyId: "",
        secretAccessKey: "",
      },
    });

    testFifoClient = new SQS({
      region: "ap-southeast-2",
      endpoint: fifoUrl,
      credentials: {
        accessKeyId: "",
        secretAccessKey: "",
      },
    });
  });

  beforeEach(() => {
    sqsQueueClient = new SqsQueueClient(queueUrl, {
      longPollDurationSeconds: 1,
      maxNumberOfMessages: 10,
      visibilityTimeoutSeconds: 5,
      region: "ap-southeast-2",
      accessKeyId: "keyId",
      secretAccessKey: "accessKey",
    });

    fifoQueueClient = new SqsQueueClient(fifoUrl, {
      longPollDurationSeconds: 1,
      maxNumberOfMessages: 10,
      visibilityTimeoutSeconds: 5,
      region: "ap-southeast-2",
      accessKeyId: "keyId",
      secretAccessKey: "accessKey",
    });
  });

  afterEach(async () => {
    jest.clearAllMocks();
    await testSqsClient.purgeQueue({ QueueUrl: queueUrl });
    await testFifoClient.purgeQueue({ QueueUrl: fifoUrl });
  });

  afterAll(async () => {
    await testSqsClient.deleteQueue({ QueueUrl: queueUrl });
    await testFifoClient.deleteQueue({ QueueUrl: fifoUrl });
    container ? await container.stop() : undefined;
  });

  const startElasticMq = async () => {
    let elasticMqEndpoint: string;

    if (!process.env.ELASTIC_MQ_URL) {
      container = await new GenericContainer("softwaremill/elasticmq-native")
        .withExposedPorts(9324)
        .start();

      const host = container.getHost();
      const port = container.getMappedPort(9324);
      elasticMqEndpoint = `http://${host}:${port}`;
    } else {
      elasticMqEndpoint = process.env.ELASTIC_MQ_URL;
    }

    console.log(`ElasticMQ running at ${elasticMqEndpoint}`);

    const sqsClient = new SQS({
      region: "ap-southeast-2",
      endpoint: elasticMqEndpoint,
      credentials: {
        accessKeyId: "",
        secretAccessKey: "",
      },
    });

    await sqsClient.send(
      new CreateQueueCommand({
        QueueName: "testQueue",
        Attributes: {
          VisibilityTimeout: "5",
        },
      })
    );

    await sqsClient.send(
      new CreateQueueCommand({
        QueueName: "fifoQueue.fifo",
        Attributes: {
          VisibilityTimeout: "5",
          FifoQueue: "true",
          ContentBasedDeduplication: "true",
        },
      })
    );

    return {
      queueUrl: `${elasticMqEndpoint}/queue/testQueue`,
      fifoUrl: `${elasticMqEndpoint}/queue/fifoQueue.fifo`,
    };
  };

  describe("Health", () => {
    it("should report the health correctly when the queue is up", async () => {
      expect(await sqsQueueClient.health()).toBe(true);
      expect(await fifoQueueClient.health()).toBe(true);
    });

    it("should fail the health check for a bad url", async () => {
      const badClient = new SqsQueueClient("bad-queue-url", {
        region: "ap-southeast-2",
        accessKeyId: "keyId",
        secretAccessKey: "accessKey",
      });

      await expect(badClient.health()).rejects.toEqual(expect.anything());
    });
  });

  describe("Send Messages", () => {
    const testMessage1 = {
      type: "test1",
      attributes: {
        stringAttribute: "stringValue",
        numberAttribute: 421,
        objectAttribute: {
          foo: "bar",
        },
        type: "this-should-get-overridden-with-test1",
      },
      body: {
        test: true,
        value: 5,
      },
    };

    const testMessage2 = {
      type: "test2",
      attributes: {} as MessageAttributes,
      body: "This body is a string",
    };

    it("should send messages to the queue", async () => {
      await sqsQueueClient.sendMessages(testMessage1, testMessage2);

      const messages = await testSqsClient.receiveMessage({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 2,
        MessageAttributeNames: ["All"],
      });

      expect(messages.Messages).toBeDefined();
      expect(messages.Messages).toHaveLength(2);

      if (!messages.Messages) {
        throw new Error("Just to make the compiler happy");
      }

      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const message1 = messages.Messages.find(
        (m) => m.MessageAttributes?.type?.StringValue === "test1"
      )!;
      const message1Attributes = message1.MessageAttributes;
      expect(message1Attributes?.type?.StringValue).toEqual("test1");
      expect(message1Attributes?.stringAttribute?.StringValue).toEqual(
        "stringValue"
      );
      expect(message1Attributes?.numberAttribute?.StringValue).toEqual("421");
      expect(message1Attributes?.objectAttribute?.BinaryValue).toEqual(
        new TextEncoder().encode(
          JSON.stringify(testMessage1.attributes.objectAttribute)
        )
      );
      expect(message1.Body).toEqual(JSON.stringify(testMessage1.body));

      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const message2 = messages.Messages.find(
        (m) => m.MessageAttributes?.type?.StringValue === "test2"
      )!;
      const message2Attributes = message2.MessageAttributes;
      expect(message2Attributes?.type?.StringValue).toEqual("test2");
      expect(message2.Body).toEqual(JSON.stringify(testMessage2.body));
    });

    it("should send messages with a delay", async () => {
      await sqsQueueClient.sendMessages({ ...testMessage1, delaySeconds: 2 });

      let messages = await testSqsClient.receiveMessage({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 1,
        MessageAttributeNames: ["All"],
      });

      expect(messages.Messages).toBeDefined();
      expect(messages.Messages).toHaveLength(0);

      await new Promise((resolve) => setTimeout(resolve, 1000));

      messages = await testSqsClient.receiveMessage({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 1,
        MessageAttributeNames: ["All"],
      });

      expect(messages.Messages).toBeDefined();
      expect(messages.Messages).toHaveLength(1);
    });

    it("should fail when attempting to send more than 10 messages", async () => {
      await expect(
        sqsQueueClient.sendMessages(
          ...Array.from({ length: 11 }).map(() => testMessage1)
        )
      ).rejects.toThrow();
    });

    it("should fail when attempting to send a message that is too large", async () => {
      await expect(
        sqsQueueClient.sendMessages({
          ...testMessage1,
          body: { payload: "a".repeat(300_000) },
        })
      ).rejects.toThrow();
    });
  });

  describe("Process messages", () => {
    let innerClientDeleteSpy: jest.SpyInstance;

    let testMessageHandler: MessageHandler<
      { payload: string },
      { foo: string }
    >;

    const testSqsMessage = {
      type: "TestMessageType",
      attributes: {
        foo: "bar",
      },
      body: {
        payload: "test",
      },
    };

    const testSnsMessage = {
      type: "",
      body: {
        Type: "Notification",
        MessageId: "test-message-id3",
        TopicArn: "arn:aws:sns:ap-southeast-2:278322397543:bla",
        Message: JSON.stringify(testSqsMessage.body),
        Timestamp: "2025-05-08T23:12:15.979Z",
        SignatureVersion: "1",
        Signature: "signature",
        SigningCertURL: "cert-url",
        UnsubscribeURL: "unsub-url",
        MessageAttributes: {
          foo: { Type: "String", Value: testSqsMessage.attributes.foo },
          type: { Type: "String", Value: testSqsMessage.type },
        },
      },
      attributes: {},
    };

    beforeEach(() => {
      testMessageHandler = {
        type: "TestMessageType",
        handleMessage: jest.fn().mockResolvedValue(undefined),
        validateMessage: jest
          .fn()
          .mockImplementation(
            (message) =>
              message.type !== undefined &&
              (message.attributes as { foo: string }).foo !== undefined &&
              (message.body as { payload: string }).payload !== undefined
          ) as unknown as MessageHandler<
          { payload: string },
          { foo: string }
        >["validateMessage"],
      };

      innerClientDeleteSpy = jest.spyOn(
        sqsQueueClient["client"],
        "deleteMessage"
      );
      sqsQueueClient.registerMessageHandler(testMessageHandler);
      fifoQueueClient.registerMessageHandler(testMessageHandler);
    });

    it.each([
      ["SQS", testSqsMessage],
      ["SNS", testSnsMessage],
    ])("should process messages from %s", async (_, message) => {
      await sqsQueueClient.sendMessages(message);
      await sqsQueueClient.pollOnceForMessages();

      expect(testMessageHandler.validateMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "TestMessageType",
          attributes: {
            type: "TestMessageType",
            foo: "bar",
          },
          body: {
            payload: "test",
          },
        })
      );
      expect(testMessageHandler.handleMessage).toHaveBeenCalledWith(
        expect.objectContaining({
          type: "TestMessageType",
          attributes: {
            type: "TestMessageType",
            foo: "bar",
          },
          body: {
            payload: "test",
          },
        })
      );

      expect(innerClientDeleteSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          QueueUrl: queueUrl,
          ReceiptHandle: expect.anything(),
        })
      );
      expect(
        (
          await testSqsClient.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 1,
          })
        ).Messages
      ).toHaveLength(0);
    });

    it("should not delete failing messages", async () => {
      (testMessageHandler.handleMessage as jest.Mock).mockRejectedValue("fail");

      await sqsQueueClient.sendMessages(testSqsMessage, {
        type: "unknown-message-type",
        attributes: {} as MessageAttributes,
        body: "This body is a string",
      });
      await sqsQueueClient.pollOnceForMessages();

      expect(testMessageHandler.validateMessage).toHaveBeenCalledTimes(1);
      expect(testMessageHandler.handleMessage).toHaveBeenCalledTimes(1);

      expect(innerClientDeleteSpy).not.toHaveBeenCalled();

      // Messages should still be invisible
      expect(
        (
          await testSqsClient.receiveMessage({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 1,
          })
        ).Messages
      ).toHaveLength(0);
    });

    it("should send and process messages on fifo/dedup queues correctly", async () => {
      await fifoQueueClient.sendMessages(
        {
          ...testSqsMessage,
          messageGroupId: "123",
        },
        {
          ...testSqsMessage,
          messageGroupId: "123",
        }, // This message should get de-duplicated by the content-based de-dup
        {
          ...testSqsMessage,
          messageGroupId: "123",
          messageDeduplicationId: "de-dup",
        },
        {
          ...testSqsMessage,
          body: { payload: "something different " },
          messageGroupId: "123",
          messageDeduplicationId: "de-dup",
        } // Should get removed based on de-dup id even though body is different
      );
      await fifoQueueClient.pollOnceForMessages();

      expect(testMessageHandler.handleMessage).toHaveBeenCalledTimes(2);
    });
  });
});
