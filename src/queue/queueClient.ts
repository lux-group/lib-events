/**
 * Metadata about a message.
 */
export type MessageAttributes = Record<string, string | number | object>;

/**
 * A message that can be placed on a queue.
 * @typeParam {Attributes} Additional metadata associated with the message.
 * @typeParam {Body} The body of the message.
 */
export type Message<Attributes extends MessageAttributes, Body> = {
  /**
   * The message type. Used to find the associated message handler.
   */
  type: string;
  /**
   * Metadata associated with the message.
   */
  attributes: Attributes;
  /**
   * Body of the message.
   */
  body: Body;
};

/**
 * A handler for a given message type to action a message of that type.
 */
export interface MessageHandler<Attributes extends MessageAttributes, Body> {
  /**
   * Type of messages to handle.
   */
  type: string;
  /**
   * Function called to handle the message.
   * @param message The message object with the matching type.
   *   It is possible that the type will be repeated in the attributes.
   */
  handleMessage: (message: Message<Attributes, Body>) => Promise<void>;
  /**
   * A function to validate that the message serialised from the message queue is of the expected type.
   * @param message A message with unknown attributes and body that needs to be validated.
   */
  validateMessage: (message: Message<MessageAttributes, unknown>) => message is Message<Attributes, Body>;
}

/**
 * A client to interact with a queue.
 * Implement this interface for a particular queue technology.
 */
export interface QueueClient {
  /**
   * Checks that the queue is reachable and healthy.
   */
  health: () => Promise<boolean>;

  /**
   * Registers a message handler to be used to process messages of the matching type.
   * @param handler A message handler.
   * @throws Error if a message handler with a given type has already been registered.
   */
  registerMessageHandler: <Attributes extends MessageAttributes, Body>(
    handler: MessageHandler<Attributes, Body>
  ) => void;

  /**
   * Starts polling for messages and processes them in the background.
   */
  startPollForMessages: () => Promise<never>;

  /**
   * Sends/adds messages to the queue (with an optional delay).
   * @param messages A message (with an optional delay) to be added to the queue.
   */
  sendMessages: (
    ...messages: (Message<MessageAttributes, unknown> & {
      delaySeconds?: number;
    })[]
  ) => Promise<void>;
}
