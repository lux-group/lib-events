import { DebugMessage, Message, PubSub, Subscription } from "@google-cloud/pubsub";

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

  constructor(private config: PubSubClientConfig) {
    this.pubSubClient = new PubSub({
      projectId: config.projectId
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
    try {
      const { onMessage, onError, onDebug, onClose } = params;
      const topic = this.pubSubClient.topic(this.config.topicName);
      
      // Create subscription if it doesn't exist
      this.subscription = topic.subscription(this.config.subscriptionName);
      const [exists] = await this.subscription.exists();
      
      if (!exists) {
        await topic.createSubscription(this.config.subscriptionName);
      }

      this.subscription.on('message', async (message: Message) => {
        //TODO: add logging and error handling - Need to verify what needs to be done based on the error or if all the errors are handled by the onError function
        await onMessage(message);
      });

      if (onDebug) {
        this.subscription.on('debug', (message: DebugMessage) => {
          onDebug(message);
        });
      }

      if (onClose) {
        this.subscription.on('close', onClose);
      }

      this.subscription.on('error', (error: Error) => {
        onError(error);
      });

    } catch (error) {
      console.error('Failed to initialize PubSub client:', error);
      throw error;
    }
  }

  async cleanup(): Promise<void> {
    if (this.subscription) {
      try {
        this.subscription.removeAllListeners();
        await this.subscription.close();
        await this.subscription.delete();
        this.subscription = null;
      } catch (error) {
        console.error('Error cleaning up subscription:', error);
        throw error;
      }
    }
  }

  async close(callback?: () => Promise<void>): Promise<void> {
    await callback?.();
    await this.cleanup();
    await this.pubSubClient.close();
  }
}

