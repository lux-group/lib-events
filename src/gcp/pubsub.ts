import {
  DebugMessage,
  Message,
  PubSub,
  Subscription,
} from "@google-cloud/pubsub";
import { AnalyticEvents } from "..";
import process from "process";

interface PubSubClientConfig {
  projectId: string;
  subscriptionName: string;
  topicName: string;
}

interface PubSubInitializeParams {
  onMessage: (message: Message) => Promise<void>;
  onError: (error: Error) => void;
  onDebug?: (message: DebugMessage) => void;
  onClose?: () => void;
  shutdownAttempts?: number;
  filterEvents?: string[];
}

export class PubSubClient {
  private pubSubClient: PubSub;
  private subscription: Subscription | null = null;
  private eventQueue = new Set<string>();
  private isShuttingDown = false;
  private initializeParams: PubSubInitializeParams = {
    onMessage: async (message: Message) => { message.ack() },
    onError: () => { throw new Error('Error occurred') },
    shutdownAttempts: 30,
    filterEvents: [],
  };

  constructor(private config: PubSubClientConfig) {
    this.pubSubClient = new PubSub({
      projectId: config.projectId,
    });
  }

  /**
   * Initialize the PubSub client and subscribe to the topic.
   * @param params.onMessage - The incoming message handler.
   * @param params.onError - The error handler.
   * @param params.onDebug - The debug message handler.
   * @param params.onClose - The close handler.
   */
  async initialize(params: PubSubInitializeParams): Promise<void> {
    this.initializeParams = { ...this.initializeParams, ...params };
    const { onMessage, onError, onDebug, onClose, filterEvents } = this.initializeParams;

    const topic = this.pubSubClient.topic(this.config.topicName);

    // Create subscription if it doesn't exist
    this.subscription = topic.subscription(this.config.subscriptionName);
    const [exists] = await this.subscription.exists();

    if (!exists) {
      await topic.createSubscription(this.config.subscriptionName);
    }

    this.subscription.on("message", async (message: Message) => {
      if (this.isShuttingDown) {
        return;
      }

      this.eventQueue.add(message.id);
      if (this._isValidEvent(message.attributes.event_name, filterEvents)) {
        await onMessage(message);
      } else {
        // Ack messages we don't process to prevent redelivery
        message.ack();
      }
      this.eventQueue.delete(message.id);
    });

    if (onDebug) {
      this.subscription.on("debug", (message: DebugMessage) => {
        onDebug(message);
      });
    }

    if (onClose) {
      this.subscription.on("close", onClose);
    }

    this.subscription.on("error", (error: Error) => {
      onError(error);
      process.exit(1);
    });
  }

  private _isValidEvent = (value: string, filterEvents?: string[]): value is AnalyticEvents => {
    //If no events were defined process everything
    if (!filterEvents?.length) {
      return true;
    }
    return filterEvents.includes(value);
  };

  private async cleanup(callback?: () => void): Promise<void> {
    let attempts = 0;

    const attemptCleanup = async (): Promise<void> => {
      if (!!this.subscription && !this._hasCurrentEvents()) {
        try {
          this.subscription.removeAllListeners();
          await this.subscription.close();
          this.subscription = null;
          callback?.();
        } catch (error) {
          throw new PubSubClientCleanupError('Error during cleanup', error as Error);
        }
      } else if (!this.subscription) {
        callback?.();
      } else if (attempts < (this.initializeParams.shutdownAttempts ?? 30)) {
        attempts++;
        setTimeout(attemptCleanup, 1000);
      } else {
        throw new PubSubClientCleanupTimeoutError(`Cleanup timed out after ${this.initializeParams.shutdownAttempts} seconds, forcing exit`);
      }
    };

    await attemptCleanup();
  }

  private _hasCurrentEvents() {
    return this.eventQueue.size > 0;
  }

  async close(callback?: () => void): Promise<void> {
    this.isShuttingDown = true;
    try {
      await this.cleanup(callback);
      await this.pubSubClient.close();
    } catch (err: any) {
      throw new PubSubClientCleanupError("Error during cleanup", err);
    }
  }
}

export class PubSubClientError extends Error {
  constructor(message: string, error?: Error) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
    if (error) {
      this.stack = this.stack + '\nCaused by: ' + error.stack;
    }
  }
}

export class PubSubClientCleanupError extends PubSubClientError {
  constructor(message: string, error?: Error) {
    super(message, error);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class PubSubClientCleanupTimeoutError extends PubSubClientError {
  constructor(message: string, error?: Error) {
    super(message, error);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class PubSubClientCloseError extends PubSubClientError {
  constructor(message: string, error?: Error) {
    super(message, error);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class PubSubClientInitializeError extends PubSubClientError {
  constructor(message: string, error?: Error) {
    super(message, error);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class PubSubClientMessageError extends PubSubClientError {
  constructor(message: string, error?: Error) {
    super(message, error);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class PubSubClientDebugError extends PubSubClientError {
  constructor(message: string, error?: Error) {
    super(message, error);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}
