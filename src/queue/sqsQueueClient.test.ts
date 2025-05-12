import { GenericContainer, StartedTestContainer } from "testcontainers";
import { CreateQueueCommand, SQS } from "@aws-sdk/client-sqs";
import { SqsQueueClient } from "./sqsQueueClient";
import { MessageAttributes, MessageHandler } from "./queueClient";

describe("SqsQueueClient", () => {
  jest.setTimeout(30_000);

  let container: StartedTestContainer;
  let queueUrl: string;

  let testSqsClient: SQS;

  let sqsQueueClient: SqsQueueClient;

  beforeAll(async () => {
    queueUrl = process.env.SQS_QUEUE_URL || "";

    if (!queueUrl) {
      queueUrl = await startElasticMq();
    }

    testSqsClient = new SQS({
      region: "ap-southeast-2",
      endpoint: queueUrl,
      credentials: {
        accessKeyId: "",
        secretAccessKey: "",
      },
    });
  });

  beforeEach(() => {
    sqsQueueClient = new SqsQueueClient(
      queueUrl,
      1,
      10,
      5,
      "ap-southeast-2",
      "keyId",
      "accessKey"
    );
  })

  afterEach(async () => {
    jest.clearAllMocks();
    await testSqsClient.purgeQueue({ QueueUrl: queueUrl });
  });

  afterAll(async () => {
    container ? await container.stop() : undefined;
  });

  const startElasticMq = async () => {
    container = await new GenericContainer("softwaremill/elasticmq-native")
      .withExposedPorts(9324)
      .start();

    const host = container.getHost();
    const port = container.getMappedPort(9324);
    const containerEndpoint = `http://${host}:${port}`;

    console.log(`ElasticMQ started at ${containerEndpoint}`);

    const sqsClient = new SQS({
      region: "ap-southeast-2",
      endpoint: containerEndpoint,
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

    return `${containerEndpoint}/queue/testQueue`;
  };

  describe("Health", () => {
    it("should report the health correctly when the queue is up", async () => {
      console.log("Queue URL: ", queueUrl);
      expect(await sqsQueueClient.health()).toBe(true);
    });

    it("should fail the health check for a bad url", async () => {
      const badClient = new SqsQueueClient(
        "bad-queue-url",
        1,
        10,
        5,
        "ap-southeast-2",
        "keyId",
        "accessKey"
      );

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
  });

  describe("Process messages", () => {
    let innerClientDeleteSpy: jest.SpyInstance;

    const testMessageHandler: MessageHandler<
      { foo: string },
      { payload: string }
    > = {
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
        { foo: string },
        { payload: string }
      >["validateMessage"],
    };

    const testMessage = {
      type: "TestMessageType",
      attributes: {
        foo: "bar",
      },
      body: {
        payload: "test",
      },
    };

    beforeEach(() => {
      innerClientDeleteSpy = jest.spyOn(
        sqsQueueClient["client"],
        "deleteMessage"
      );
      sqsQueueClient.registerMessageHandler(testMessageHandler);
    });

    it("should process messages", async () => {
      await sqsQueueClient.sendMessages(testMessage);
      await sqsQueueClient["pollOnceForMessages"]();

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

      await sqsQueueClient.sendMessages(testMessage, {
        type: "unknown-message-type",
        attributes: {} as MessageAttributes,
        body: "This body is a string",
      });
      await sqsQueueClient["pollOnceForMessages"]();

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
  });
});
