import { PubSub } from "@google-cloud/pubsub";
import { PubSubClient } from "./pubsub";

describe("PubSubClient", () => {
  let pubSubClient: PubSubClient;
  const mockConfig = {
    projectId: "test-project",
    subscriptionName: "test-subscription", 
    topicName: "test-topic"
  };

  beforeEach(() => {
    pubSubClient = new PubSubClient(mockConfig);
  });

  afterEach(async () => {
    await pubSubClient.cleanup();
  });

  describe("initialize", () => {
    it("should initialize subscription and attach handlers", async () => {
      const mockMessageHandler = jest.fn();
      const mockErrorHandler = jest.fn();
      const mockDebugHandler = jest.fn();
      const mockCloseHandler = jest.fn();

      const mockSubscription = {
        exists: jest.fn().mockResolvedValue([true]),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        close: jest.fn(),
        delete: jest.fn()
      };

      const mockTopic = {
        subscription: jest.fn().mockReturnValue(mockSubscription),
        createSubscription: jest.fn()
      };

      (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

      await pubSubClient.initialize({
        onMessage: mockMessageHandler,
        onError: mockErrorHandler,
        onDebug: mockDebugHandler,
        onClose: mockCloseHandler
      });

      expect(mockTopic.subscription).toHaveBeenCalledWith(mockConfig.subscriptionName);
      expect(mockSubscription.exists).toHaveBeenCalled();
      expect(mockSubscription.on).toHaveBeenCalledWith("message", expect.any(Function));
      expect(mockSubscription.on).toHaveBeenCalledWith("error", expect.any(Function));
      expect(mockSubscription.on).toHaveBeenCalledWith("debug", expect.any(Function));
      expect(mockSubscription.on).toHaveBeenCalledWith("close", mockCloseHandler);
    });

    it("should create subscription if it doesn't exist", async () => {
      const mockSubscription = {
        exists: jest.fn().mockResolvedValue([false]),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        close: jest.fn(),
        delete: jest.fn()
      };

      const mockTopic = {
        subscription: jest.fn().mockReturnValue(mockSubscription),
        createSubscription: jest.fn()
      };

      (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

      await pubSubClient.initialize({
        onMessage: jest.fn(),
        onError: jest.fn(),
        onDebug: jest.fn(),
        onClose: jest.fn()
      });

      expect(mockTopic.createSubscription).toHaveBeenCalledWith(mockConfig.subscriptionName);
    });

    it("should register debug handler only if debugHandler is provided", async () => {
      const mockSubscription = {
        exists: jest.fn().mockResolvedValue([true]),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        close: jest.fn(),
        delete: jest.fn()
      };

      const mockTopic = {
        subscription: jest.fn().mockReturnValue(mockSubscription),
        createSubscription: jest.fn()
      };

      (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

      await pubSubClient.initialize({
        onMessage: jest.fn(),
        onError: jest.fn(),
        onClose: jest.fn()
      });

      expect(mockSubscription.on).not.toHaveBeenCalledWith("debug", expect.any(Function));
    });
  });

    it("should register close handler only if closeHandler is provided", async () => {
      const mockSubscription = {
        exists: jest.fn().mockResolvedValue([true]),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        close: jest.fn(),
        delete: jest.fn()
      };

      const mockTopic = {
        subscription: jest.fn().mockReturnValue(mockSubscription),
        createSubscription: jest.fn()
      };

      (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

      await pubSubClient.initialize({
        onMessage: jest.fn(),
        onError: jest.fn()
      });

      expect(mockSubscription.on).not.toHaveBeenCalledWith("close", expect.any(Function));
    });

  it("should throw an error if initialization fails", async () => {
    const mockSubscription = {
      exists: jest.fn().mockRejectedValue(new Error("Initialization failed")),
      on: jest.fn(),
      removeAllListeners: jest.fn(),
      close: jest.fn(),
      delete: jest.fn()
    };

    const mockTopic = {
      subscription: jest.fn().mockReturnValue(mockSubscription),
      createSubscription: jest.fn()
    };

    (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

    await expect(pubSubClient.initialize({
      onMessage: jest.fn(),
      onError: jest.fn()
    })).rejects.toThrow("Initialization failed");

    expect(mockSubscription.exists).toHaveBeenCalled();
  });


  describe("cleanup", () => {
    it("should cleanup subscription properly", async () => {
      const mockSubscription = {
        exists: jest.fn().mockResolvedValue([true]),
        on: jest.fn(),
        removeAllListeners: jest.fn(),
        close: jest.fn(),
        delete: jest.fn()
      };

      const mockTopic = {
        subscription: jest.fn().mockReturnValue(mockSubscription),
        createSubscription: jest.fn()
      };

      (PubSub.prototype as any).topic = jest.fn().mockReturnValue(mockTopic);

      await pubSubClient.initialize({
        onMessage: jest.fn(),
        onError: jest.fn(),
        onDebug: jest.fn(),
        onClose: jest.fn()
      });

      await pubSubClient.cleanup();

      expect(mockSubscription.removeAllListeners).toHaveBeenCalled();
      expect(mockSubscription.close).toHaveBeenCalled();
      expect(mockSubscription.delete).toHaveBeenCalled();
    });
  });

  describe("close", () => {
    it("should call cleanup and close pubsub client", async () => {
      const mockCallback = jest.fn();
      const mockClose = jest.spyOn(PubSub.prototype, "close");
      
      await pubSubClient.close(mockCallback);

      expect(mockCallback).toHaveBeenCalled();
      expect(mockClose).toHaveBeenCalled();
    });
  });
});
