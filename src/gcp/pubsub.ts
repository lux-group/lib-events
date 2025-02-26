import {
  DebugMessage,
  Message,
  PubSub, StatusError,
  Subscription,
} from "@google-cloud/pubsub";
import { AnalyticEvents } from "..";


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

export class PubSubClientMessageError extends PubSubClientError {
  constructor(message: string, error?: Error) {
    super(message, error);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

interface Credentials {
  client_email: string;
  private_key: string;
}

interface PubSubClientConfig {
  projectId: string;
  subscriptionName: string;
  topicName: string;
  credentials?: Credentials;
}

/**
 * @throws {PubSubClientMessageError}
 */
type OnMessageHandler = (message: Message) => Promise<void>;

interface PubSubInitializeParams {
  onMessage: OnMessageHandler;
  onError?: (error: StatusError) => void;
  onDebug?: (message: DebugMessage) => void;
  onClose?: () => void;
  shutdownTimeout?: number;
  filterEvents?: string[];
  logger?: (message: string, ...meta: any[]) => void;
}

export class PubSubClient {
  private _defaultErrorHandler = (error: StatusError) => {
    const logger = this.initializeParams.logger ?? this._defaultLogger;
    if (error.code === 4) { // Deadline exceeded
      logger('Deadline exceeded. Retrying...', { error });
    } else if (error.code === 3){ // Unavailable
      logger("Service unavailable. Retrying...", { error });
    } else if (error.code === 5){ // Topic not found
      logger("Subscription or topic not found.", { error });
    } else if (error.code === 7){ // Permission denied
      logger("Permission denied.", { error });
    } else if (error.code === 9){ // Invalid argument
      logger("Invalid argument.", { error });
    } else { // Unhandled / Unknown error
      logger("Unhandled error.", { error });
    }
  }

  private _defaultDebugger = (message: DebugMessage) => {
    const logger = this.initializeParams.logger ?? this._defaultLogger;
    logger('Debug message received', { message });
  }

  private _defaultLogger = (message: string, ...meta: any[]) => {
    console.info(message, meta);
  }

  private pubSubClient: PubSub;
  private subscription: Subscription | null = null;
  private isProcessing = false;
  private isShuttingDown = false;
  private initializeParams: PubSubInitializeParams = {
    onMessage: async (message: Message) => { message.ack() },
    onError: this._defaultErrorHandler.bind(this),
    onDebug: this._defaultDebugger.bind(this),
    shutdownTimeout: 30,
    filterEvents: [],
    logger: this._defaultLogger.bind(this),
  };

  constructor(private config: PubSubClientConfig) {
    this.pubSubClient = new PubSub({
      projectId: config.projectId,
      credentials: config.credentials
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

    this.subscription = topic.subscription(this.config.subscriptionName);
    const [exists] = await this.subscription.exists();

    if (!exists) {
      await topic.createSubscription(this.config.subscriptionName);
    }

    this.subscription.on("message", async (message: Message) => {
      if (this.isShuttingDown) {
        return;
      }

      this.isProcessing = true;
      if (this._isValidEvent(message.attributes.event_name, filterEvents)) {
        try {
          await onMessage(message);
          message.ack();
        } catch (error) {
          message.nack();
        }
      }
      this.isProcessing = false;
    });

    if (onDebug) {
      this.subscription.on("debug", (message: DebugMessage) => {
        onDebug(message);
      });
    }

    if (onClose) {
      this.subscription.on("close", onClose);
    }

    if (onError) {
      this.subscription.on("error", onError);
    }
  }

  private _isValidEvent = (value: string, filterEvents?: string[]): value is AnalyticEvents => {
    //If no events were defined process everything
    if (!filterEvents?.length) {
      return true;
    }
    return filterEvents.includes(value);
  };

  /**
   * Close the PubSub client and cleanup resources.
   * @param callback - A callback from the service layer that will be called after cleanup. ex: () => process.exit(0)
   * @private
   */
  private async _cleanup(callback?: () => void): Promise<void> {
    let attempts = 0;

    const attemptCleanup = async (): Promise<void> => {
      if (!!this.subscription && !this.isProcessing) {
        try {
          this.subscription.removeAllListeners();
          await this.subscription.close();
          this.subscription = null;

        } catch (error) {
          throw new PubSubClientCleanupError('Error during cleanup', error as Error);
        } finally {
          callback?.();
        }
      } else if (!this.subscription) {
        callback?.();
      } else if (attempts < (this.initializeParams.shutdownTimeout ?? 30)) {
        attempts++;
        setTimeout(attemptCleanup, 1000);
      } else {
        callback?.();
        throw new PubSubClientCleanupTimeoutError(`Cleanup timed out after ${this.initializeParams.shutdownTimeout} seconds, forcing exit`);
      }
    };

    await attemptCleanup();
  }

  async close(callback?: () => void): Promise<void> {
    this.isShuttingDown = true;
    try {
      await this._cleanup(callback);
      await this.pubSubClient.close();
    } catch (err: any) {
      throw new PubSubClientCleanupError("Error during cleanup", err);
    }
  }
}
