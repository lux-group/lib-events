export type MessageAttributes = Record<string, string | number | object>;

export type Message<Attributes extends MessageAttributes, Body> = {
  type: string;
  attributes: Attributes;
  body: Body;
};

export interface MessageHandler<Attributes extends MessageAttributes, Body> {
  type: string;
  handleMessage: (message: Message<Attributes, Body>) => Promise<void>;
  validateMessage: (message: Message<MessageAttributes, unknown>) => message is Message<Attributes, Body>;
}

export interface QueueClient {
  health: () => Promise<boolean>;
  registerMessageHandler: <Attributes extends MessageAttributes, Body>(
    handler: MessageHandler<Attributes, Body>
  ) => void;
  startPollForMessages: () => Promise<never>;
  sendMessages: (
    ...messages: (Message<MessageAttributes, unknown> & {
      delaySeconds?: number;
    })[]
  ) => Promise<void>;
}
