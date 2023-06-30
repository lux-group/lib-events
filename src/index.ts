import AWS from "aws-sdk";

import { encodeJson, decodeJson } from "./base64";

export type Message<T = undefined> = {
  type: string;
  source: string;
  id?: string;
  uri?: string;
  checksum: number;
  message: string;
  json?: T;
  transactionId?: string;
  groupId?: string;
};

interface ConsumerParams {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  region: string;
  queueUrl: string;
}

type ProcessMessage = (getAttributes: (body: string) => Message, deleteMessage: (message: Message) => void) => Message

interface Consumer {
  poll: (processMessage: ProcessMessage, param?: { maxNumberOfMessages: number, maxIterations: number }) => Promise<void>;
  getAttributes: (json: string) => Message
  mapAttributes: (data: {
    MessageAttributes: { [key: string]: { Value: string } }
  }) => Message
}

interface PublisherParams {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  region: string;
  topic: string;
  apiHost: string;
}

interface Publisher {
  dispatch: (message: Message) => Promise<void | AWS.SNS.Types.PublishResponse>
}

interface ReceiveMessageParams {
  QueueUrl: string;
  MaxNumberOfMessages: number;
}

interface MessageAttribute {
  DataType: string;
  StringValue: string;
}

interface MessageAttributes {
  type: MessageAttribute;
  checksum: MessageAttribute;
  source: MessageAttribute;
  uri?: MessageAttribute;
  id?: MessageAttribute;
  json?: MessageAttribute;
}

interface Credentials {
  apiVersion: string;
  accessKeyId: string;
  secretAccessKey: string;
  region: string;
  sessionToken?: string;
}

interface PollParams {
  maxNumberOfMessages?: number
  maxIterations?: number
}

// Amazon SNS currently allows a maximum size of 256 KB for published messages.
const MAX_EVENT_MESSAGE_SIZE = 256 * 1024;

export enum Events {
  ORDER_PENDING = "ORDER_PENDING",
  ORDER_COMPLETED = "ORDER_COMPLETED",
  ORDER_CANCELLED = "ORDER_CANCELLED",
  ORDER_REFUNDED = "ORDER_REFUNDED",
  ORDER_ABANDONED = "ORDER_ABANDONED",
  ORDER_AWAITING_PAYMENT = "ORDER_AWAITING_PAYMENT",
  ORDER_AWAITING_PURCHASE = "ORDER_AWAITING_PURCHASE",
  ORDER_NEEDS_ATTENTION = "ORDER_NEEDS_ATTENTION",
  ORDER_PAYMENT_FAILED = "ORDER_PAYMENT_FAILED",
  ORDER_TOUCH = "ORDER_TOUCH",

  ORDERS_CHECKSUM = "ORDERS_CHECKSUM",
  ORDERS_CHECKSUM_ERROR = "ORDERS_CHECKSUM_ERROR",

  ORDER_ITEM_CREATED = "ORDER_ITEM_CREATED",
  ORDER_ITEM_COMPLETED = "ORDER_ITEM_COMPLETED",
  ORDER_ITEM_AWAITING_DATES = "ORDER_ITEM_AWAITING_DATES",
  ORDER_ITEM_FAILED = "ORDER_ITEM_FAILED",
  ORDER_ITEM_CANCELLED = "ORDER_ITEM_CANCELLED",
  ORDER_ITEM_TOUCH = "ORDER_ITEM_TOUCH",
  ORDER_ITEM_CHANGE_DATES = "ORDER_ITEM_CHANGE_DATES",
  ORDER_ITEM_DELETE_RESERVATION = "ORDER_ITEM_DELETE_RESERVATION",
  ORDER_ITEM_UPDATE_RESERVATION = "ORDER_ITEM_UPDATE_RESERVATION",
  ORDER_ITEM_REFUND = "ORDER_ITEM_REFUND",
  ORDER_ITEMS_CHECKSUM = "ORDER_ITEMS_CHECKSUM",
  ORDER_ITEMS_CHECKSUM_ERROR = "ORDER_ITEMS_CHECKSUM_ERROR",

  ORDER_ADDON_ITEM_CANCELLED = "ORDER_ADDON_ITEM_CANCELLED",

  ORDER_FLIGHT_ITEM_CREATED = "ORDER_FLIGHT_ITEM_CREATED",
  ORDER_FLIGHT_ITEM_COMPLETED = "ORDER_FLIGHT_ITEM_COMPLETED",
  ORDER_FLIGHT_ITEM_FAILED = "ORDER_FLIGHT_ITEM_FAILED",
  ORDER_FLIGHT_ITEM_CANCELLED = "ORDER_FLIGHT_ITEM_CANCELLED",

  OFFER_UPDATE = "OFFER_UPDATE",
  OFFER_LOWEST_PRICE_UPDATE = "OFFER_LOWEST_PRICE_UPDATE",

  RATE_PLAN_UPDATE = "RATE_PLAN_UPDATE",
  RATE_PLAN_DELETE = "RATE_PLAN_DELETE",

  PROPERTY_UPDATE = "PROPERTY_UPDATE",
  PROPERTY_DELETE = "PROPERTY_DELETE",

  PROPERTY_PARENT_UPDATE = "PROPERTY_PARENT_UPDATE",

  BEDBANK_PROPERTY_RATING_UPDATE = "BEDBANK_PROPERTY_RATING_UPDATE",
  PROPERTY_RATING_UPDATE = "PROPERTY_RATING_UPDATE",
  EXPERIENCE_RATING_UPDATE = "EXPERIENCE_RATING_UPDATE",

  ROOM_AVAILABILITY_UPDATE = "ROOM_AVAILABILITY_UPDATE",
  RATE_AVAILABILITY_UPDATE = "RATE_AVAILABILITY_UPDATE",

  HOTEL_RESERVATION_SITEMINDER_ERROR = "HOTEL_RESERVATION_SITEMINDER_ERROR",
  HOTEL_RESERVATION_TRAVELCLICK_ERROR = "HOTEL_RESERVATION_TRAVELCLICK_ERROR",

  RESERVATION_FX_RATES_UPDATE = "RESERVATION_FX_RATES_UPDATE",

  SITEMINDER_CURRENCY_ERROR = "SITEMINDER_CURRENCY_ERROR",

  VOUCHER_UPDATE = "VOUCHER_UPDATE",

  TOUR_OFFER_UPDATE = "TOUR_OFFER_UPDATE",
  TOUR_UPDATE = "TOUR_UPDATE",
  TOUR_DELETE = "TOUR_DELETE",

  CONN_SF_TOUR_UPDATE = "CONN_SF_TOUR_UPDATE",

  GDPR_REMOVAL = "GDPR_REMOVAL",

  ARI_RATES_UPDATE = "ARI_RATES_UPDATE",
  ARI_INVENTORY_UPDATE = "ARI_INVENTORY_UPDATE",
  ARI_AVAILABILITY_UPDATE = "ARI_AVAILABILITY_UPDATE",

  BEDBANK_PROPERTY_FLIGHT_UPDATE = "BEDBANK_PROPERTY_FLIGHT_UPDATE",
  BEDBANK_SYNC = "BEDBANK_SYNC",
  BEDBANK_UPDATE = "BEDBANK_UPDATE",

  CRUISE_SYNC = "CRUISE_SYNC",
  CRUISE_UPDATE = "CRUISE_UPDATE",

  USER_SIGN_UP = "USER_SIGN_UP",

  CAR_HIRE_LOCATION_SYNC = "CAR_HIRE_LOCATION_SYNC",
};

export class InvalidEventTypeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class InvalidEventChecksumError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class InvalidEventSourceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class InvalidEventMessageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class InvalidEventJsonError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class InvalidEventSizeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export class InvalidFIFOMessageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

export function createPublisher({
  accessKeyId,
  secretAccessKey,
  sessionToken,
  region,
  topic,
  apiHost,
}: PublisherParams): Publisher {
  const credentials: Credentials = {
    apiVersion: "2010-03-31",
    accessKeyId,
    secretAccessKey,
    region,
  };

  if (sessionToken) {
    credentials.sessionToken = sessionToken;
  }

  const sns = new AWS.SNS(credentials);
  const isFIFO = topic.endsWith(".fifo");

  function dispatch({
    type,
    uri,
    id,
    checksum,
    source,
    message,
    json,
    groupId,
    transactionId,
  }: Message): Promise<void| AWS.SNS.Types.PublishResponse> {
    if (!Object.values<string>(Events).includes(type)) {
      throw new InvalidEventTypeError(`invalid event type '${type}'`);
    }

    if (isNaN(checksum)) {
      throw new InvalidEventChecksumError("checksum is not a number");
    }

    if (!source) {
      throw new InvalidEventSourceError("event source is required");
    }

    if (!message) {
      throw new InvalidEventMessageError("event message is required");
    }

    if (isFIFO && !groupId) {
      throw new InvalidFIFOMessageError(
        "groupId is required for FIFO messages"
      );
    }

    if (isFIFO && !transactionId) {
      throw new InvalidFIFOMessageError(
        "transactionId is required for FIFO messages"
      );
    }

    const messageAttributes: MessageAttributes = {
      type: {
        DataType: "String",
        StringValue: type,
      },
      checksum: {
        DataType: "Number",
        StringValue: checksum.toString(),
      },
      source: {
        DataType: "String",
        StringValue: source,
      },
    };

    if (uri) {
      messageAttributes.uri = {
        DataType: "String",
        StringValue: `${apiHost}${uri}`,
      };
    }

    if (id) {
      messageAttributes.id = {
        DataType:"String",
        StringValue: id,
      };
    }

    if (json) {
      try {
        messageAttributes.json = {
          DataType: "String",
          StringValue: encodeJson<Record<string, string>>(json),
        };
      } catch (e) {
        throw new InvalidEventJsonError("event json is invalid");
      }
    }

    const eventParams: AWS.SNS.Types.PublishInput = {
      MessageAttributes: { ...messageAttributes },
      TopicArn: topic,
      Message: message,
    };

    if (
      Buffer.byteLength(JSON.stringify(eventParams), "utf8") >
      MAX_EVENT_MESSAGE_SIZE
    ) {
      throw new InvalidEventSizeError("json message exceeded limit of 256KB");
    }

    if (groupId) {
      eventParams.MessageGroupId = groupId;
    }

    if (transactionId) {
      eventParams.MessageDeduplicationId = transactionId;
    }

    if (process.env.IGNORE_EVENTS == "true") {
      return Promise.resolve();
    }

    return sns.publish(eventParams).promise();
  }

  return {
    dispatch,
  };
}

export function createConsumer({
  accessKeyId,
  secretAccessKey,
  sessionToken,
  region,
  queueUrl,
}: ConsumerParams): Consumer {
  const credentials: Credentials = {
    apiVersion: "2012-11-05",
    accessKeyId,
    secretAccessKey,
    region,
  };

  if (sessionToken) {
    credentials.sessionToken = sessionToken;
  }
  const sqs = new AWS.SQS(credentials);

  function deleteMessage(message: AWS.SQS.Types.Message) {
    return function ack() {
      return new Promise((accept, reject) => {
        if (!message.ReceiptHandle) {
          return reject(
            new InvalidEventMessageError('invalid ReceiptHandle')
          )
        }

        sqs.deleteMessage(
          {
            QueueUrl: queueUrl,
            ReceiptHandle: message.ReceiptHandle,
          },
          (err: Error | undefined, response: Record<string, never>) => {
            if (err) {
              return reject(err);
            }
            return accept({
              response,
              message,
            });
          }
        );
      });
    };
  }

  function getAttributes(body: AWS.SQS.Types.Message['Body']) {
    if (!body) {
      return {}
    }

    const bodyJson = JSON.parse(body);
    // handle s3 upload event
    // currently we can only one record per s3 event
    // https =//stackoverflow.com/questions/40765699/how-many-records-can-be-in-s3-put-event-lambda-trigger/40767563#40767563
    if (bodyJson.Records) {
      return bodyJson.Records[0];
    }

    else if (bodyJson.MessageAttributes) {
      // handle sns message
      return mapAttributes(bodyJson);
    }

    // do nothing if the message type it not what we need
    return {};
  }

  function mapAttributes(data: { MessageAttributes: { [key: string]: { Value: string } } }): Message {
    const message: Message = {
      type: data.MessageAttributes.type.Value,
      source: data.MessageAttributes.source.Value,
      checksum: Number(data.MessageAttributes.checksum.Value),
      message: data.MessageAttributes.message.Value
    }

    if (data.MessageAttributes.id) {
      message.id = data.MessageAttributes.id.Value
    }

    if (data.MessageAttributes.uri) {
      message.uri = data.MessageAttributes.uri.Value
    }

    if (data.MessageAttributes.json) {
      message.json = decodeJson(data.MessageAttributes.json.Value)
    }

    if (data.MessageAttributes.transactionId) {
      message.transactionId = data.MessageAttributes.transactionId.Value
    }

    if (data.MessageAttributes.id) {
      message.groupId = data.MessageAttributes.groupId.Value
    }

    return message
  }

  function receiveMessages(processMessage: ProcessMessage, data: AWS.SQS.Types.ReceiveMessageResult): Message[] {
    if (!data.Messages || data.Messages.length === 0) {
      return [];
    }
    return data.Messages.map((message: AWS.SQS.Types.Message) => {
      return processMessage(
        getAttributes(message.Body),
        deleteMessage(message)
      );
    });
  }

  function wait(processMessage: ProcessMessage, receiveMessageParams: ReceiveMessageParams): Promise<Message[]> {
    return new Promise((accept, reject) => {
      sqs.receiveMessage(receiveMessageParams, (err: Error | null, data: AWS.SQS.Types.ReceiveMessageResult) => {
        if (err) {
          return reject(err);
        }
        Promise.all(receiveMessages(processMessage, data))
          .then(accept)
          .catch(reject);
      });
    });
  }

  function pollStart(
    processMessage: ProcessMessage,
    n: number,
    t: number,
    receiveMessageParams: ReceiveMessageParams
  ): Promise<void> {
    if (t >= n) {
      return Promise.resolve();
    }
    return wait(processMessage, receiveMessageParams).then((results) => {
      if (results.length == 0) {
        return Promise.resolve();
      }
      return poll(processMessage, n, t + 1, receiveMessageParams);
    });
  }

  function poll(
    processMessage: ProcessMessage,
    n: number,
    t: number,
    receiveMessageParams: ReceiveMessageParams
  ): Promise<void> {
    return pollStart(
      processMessage,
      n,
      t,
      receiveMessageParams
    );
  }

  return {
    poll: (processMessage: ProcessMessage, params?: PollParams) => (
      poll(
        processMessage,
        0,
        params?.maxIterations ?? 10,
        { QueueUrl: queueUrl, MaxNumberOfMessages: params?.maxNumberOfMessages ?? 10 }
      )
    ),
    getAttributes,
    mapAttributes,
  };
}
