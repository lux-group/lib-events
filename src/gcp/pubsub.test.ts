import { PubSub } from "@google-cloud/pubsub";
import { PubSubClient } from "./pubsub";

jest.mock("process", () => ({
  exit: jest.fn(),
}));
jest.mock("./pubsub", () => ({
  ...jest.requireActual("./pubsub"),
  _hasCurrentEvents: jest.fn().mockReturnValue(false),
}));

describe("PubSubClient", () => {
  let pubSubClient: PubSubClient;
  const mockConfig = {
    projectId: "test-project",
    subscriptionName: "test-subscription",
    topicName: "test-topic",
  };

  beforeEach(() => {
    pubSubClient = new PubSubClient(mockConfig);
  });

  afterEach(async () => {
    await pubSubClient.close();
  });

  describe("initialize", () => {
    it("should initialize subscription and attach handlers", async () => {
      const mockMessageHandler = jest.fn().mockName("onMessage");
      const mockErrorHandler = jest.fn().mockName("onError");
      const mockDebugHandler = jest.fn().mockName("onDebug");
      const mockCloseHandler = jest.fn().mockName("onClose");

      const mockSubscription = {
        exists: jest.fn().mockResolvedValue([true]),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        close: jest.fn(),
        delete: jest.fn(),
      };

      const mockTopic = {
        subscription: jest.fn().mockReturnValue(mockSubscription),
        createSubscription: jest.fn(),
      };

      (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

      await pubSubClient.initialize({
        onMessage: mockMessageHandler,
        onError: mockErrorHandler,
        onDebug: mockDebugHandler,
        onClose: mockCloseHandler,
      });

      expect(mockTopic.subscription).toHaveBeenCalledWith(
        mockConfig.subscriptionName
      );
      expect(mockSubscription.exists).toHaveBeenCalled();
      expect(mockSubscription.on).toHaveBeenCalledWith(
        "message",
        expect.any(Function)
      );
      expect(mockSubscription.on).toHaveBeenCalledWith(
        "error",
        expect.any(Function)
      );
      expect(mockSubscription.on).toHaveBeenCalledWith(
        "debug",
        expect.any(Function)
      );
      expect(mockSubscription.on).toHaveBeenCalledWith(
        "close",
        expect.any(Function)
      );
    });

    it("should create subscription if it doesn't exist", async () => {
      const mockSubscription = {
        exists: jest.fn().mockResolvedValue([false]),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        close: jest.fn(),
        delete: jest.fn(),
      };

      const mockTopic = {
        subscription: jest.fn().mockReturnValue(mockSubscription),
        createSubscription: jest.fn(),
      };

      (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

      await pubSubClient.initialize({
        onMessage: jest.fn(),
        onError: jest.fn(),
        onDebug: jest.fn(),
        onClose: jest.fn(),
      });

      expect(mockTopic.createSubscription).toHaveBeenCalledWith(
        mockConfig.subscriptionName
      );
    });

    it("should throw an error if initialization fails", async () => {
      const mockSubscription = {
        exists: jest.fn().mockRejectedValue(new Error("Initialization failed")),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        close: jest.fn(),
        delete: jest.fn(),
      };

      const mockTopic = {
        subscription: jest.fn().mockReturnValue(mockSubscription),
        createSubscription: jest.fn(),
      };

      (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

      await expect(
        pubSubClient.initialize({
          onMessage: jest.fn(),
          onError: jest.fn(),
        })
      ).rejects.toThrow("Initialization failed");

      expect(mockSubscription.exists).toHaveBeenCalled();
    });
  });

  describe("close", () => {
    it("should call removeAllListeners, close and delete pubsub client", async () => {
      const mockSubscription = {
        exists: jest.fn().mockResolvedValue([true]),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        close: jest.fn(),
        delete: jest.fn(),
      };

      const mockTopic = {
        subscription: jest.fn().mockReturnValue(mockSubscription),
        createSubscription: jest.fn(),
      };

      (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

      await pubSubClient.initialize({
        onMessage: jest.fn(),
        onError: jest.fn(),
        onDebug: jest.fn(),
        onClose: jest.fn(),
      });

      await pubSubClient.close();

      expect(mockSubscription.removeAllListeners).toHaveBeenCalled();
      expect(mockSubscription.close).toHaveBeenCalled();
    });
  });
});
