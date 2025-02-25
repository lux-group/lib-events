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
}

export class PubSubClient {
  private pubSubClient: PubSub;
  private subscription: Subscription | null = null;
  private eventQueue = new Set<string>();
  private isShuttingDown = false;
  private hasShutDown = false;

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
    const { onMessage, onError, onDebug, onClose } = params;
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
      if (this._isValidEvent(message.attributes.event_name)) {
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

  private _isValidEvent = (value: string): value is AnalyticEvents => {
    return [
      AnalyticEvents.PRODUCT_IMPRESSION,
      AnalyticEvents.PRODUCT_CLICK,
      AnalyticEvents.PRODUCT_PURCHASE,
    ].includes(value as AnalyticEvents);
  };

  private async cleanup(): Promise<void> {
    const MAX_RETRY_ATTEMPTS = 30; // 30 seconds max wait
    let attempts = 0;

    const attemptCleanup = async (): Promise<void> => {
      if (!!this.subscription && !this._hasCurrentEvents()) {
        try {
          this.subscription.removeAllListeners();
          await this.subscription.close();
          this.subscription = null;
          process.exit(0);
        } catch (error) {
          console.error('Error during cleanup:', error);
          process.exit(1);
        }
      } else if (!this.subscription) {
        console.error('Cleanup finished')
        process.exit(0);
      } else if (attempts < MAX_RETRY_ATTEMPTS) {
        attempts++;
        setTimeout(attemptCleanup, 1000);
      } else {
        console.warn('Cleanup timed out after 30 seconds, forcing exit');
        process.exit(1);
      }
    };

    await attemptCleanup();
  }

  private _hasCurrentEvents() {
    return this.eventQueue.size > 0;
  }

  async close(): Promise<void> {
    this.isShuttingDown = true;
    try {
      await this.cleanup();
      await this.pubSubClient.close();
      process.exit(0);
    } catch (err: any) {
      Error.captureStackTrace(err);
      process.exit(1);
    }
  }
}
