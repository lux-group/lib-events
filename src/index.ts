import { Credentials } from "@aws-sdk/types";
import {
  SNSClient,
  PublishCommand,
  PublishCommandInput,
  PublishCommandOutput,
} from "@aws-sdk/client-sns";
import {
  SQSClient,
  DeleteMessageCommand,
  DeleteMessageCommandInput,
  ReceiveMessageCommand,
  ReceiveMessageCommandOutput,
  Message as SQSMessage,
} from "@aws-sdk/client-sqs";

import { encodeJson, decodeJson } from "./base64";

export type Message<T = unknown> = {
  type: string;
  source: string;
  id?: string;
  uri?: string;
  checksum: number;
  message: string;
  json?: T;
  transactionId?: string;
  groupId?: string;
  brand?: string;
};

interface ConsumerParams {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
  region: string;
  queueUrl: string;
}

type ProcessMessage = (
  message: Message,
  ack: () => Promise<void>
) => Promise<void>;

interface Consumer {
  poll: (
    processMessage: ProcessMessage,
    param?: { maxNumberOfMessages: number; maxIterations: number }
  ) => Promise<void>;
  getAttributes: (json: string) => Message;
  mapAttributes: (data: {
    Message: string;
    MessageAttributes: { [key: string]: { Value: string } };
  }) => Message;
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
  dispatch: (message: Message) => Promise<void | PublishCommandOutput>;
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

interface PollParams {
  maxNumberOfMessages?: number;
  maxIterations?: number;
}

// Amazon SNS currently allows a maximum size of 256 KB for published messages.
const MAX_EVENT_MESSAGE_SIZE = 256 * 1024;

const DEFAULT_MAX_ITERATIONS = 10;

const DEFAULT_MAX_NUMBER_OF_MESSAGES = 10;

export enum Events {
  ORDER_PENDING = "ORDER_PENDING",
  ORDER_COMPLETED = "ORDER_COMPLETED",
  ORDER_CANCELLED = "ORDER_CANCELLED",
  ORDER_FAILED = "ORDER_FAILED",
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
  OFFER_TAG_UPDATE = "OFFER_TAG_UPDATE",

  RATE_PLAN_UPDATE = "RATE_PLAN_UPDATE",
  RATE_PLAN_DELETE = "RATE_PLAN_DELETE",

  PROPERTY_UPDATE = "PROPERTY_UPDATE",
  PROPERTY_DELETE = "PROPERTY_DELETE",

  PROPERTY_PARENT_UPDATE = "PROPERTY_PARENT_UPDATE",

  BEDBANK_PROPERTY_RATING_UPDATE = "BEDBANK_PROPERTY_RATING_UPDATE",
  PROPERTY_RATING_UPDATE = "PROPERTY_RATING_UPDATE",
  EXPERIENCE_RATING_UPDATE = "EXPERIENCE_RATING_UPDATE",
  TOUR_RATING_UPDATE = "TOUR_RATING_UPDATE",
  RATING_REVIEW_CREATED = "RATING_REVIEW_CREATED",
  RATING_REVIEW_UPDATED = "RATING_REVIEW_UPDATED",

  ROOM_AVAILABILITY_UPDATE = "ROOM_AVAILABILITY_UPDATE",
  RATE_AVAILABILITY_UPDATE = "RATE_AVAILABILITY_UPDATE",

  HOTEL_RESERVATION_SITEMINDER_ERROR = "HOTEL_RESERVATION_SITEMINDER_ERROR",
  HOTEL_RESERVATION_TRAVELCLICK_ERROR = "HOTEL_RESERVATION_TRAVELCLICK_ERROR",

  RESERVATION_FX_RATES_UPDATE = "RESERVATION_FX_RATES_UPDATE",
  FX_RATE_UPDATE = "FX_RATE_UPDATE",

  RESERVATION_UPDATE = "RESERVATION_UPDATE",
  RESERVATION_CONFIRM_FAILURE = "RESERVATION_CONFIRM_FAILURE",

  SITEMINDER_CURRENCY_ERROR = "SITEMINDER_CURRENCY_ERROR",

  RENTALSUNITED_PROPERTY_UPDATE = "RENTALSUNITED_PROPERTY_UPDATE",
  RENTALSUNITED_REVIEW_UPDATE = "RENTALSUNITED_REVIEW_UPDATE",
  RENTALSUNITED_IMAGES_COMPLETE = "RENTALSUNITED_IMAGES_COMPLETE",

  VOUCHER_UPDATE = "VOUCHER_UPDATE",

  TOUR_OFFER_UPDATE = "TOUR_OFFER_UPDATE",
  TOUR_UPDATE = "TOUR_UPDATE",
  TOUR_DELETE = "TOUR_DELETE",
  TOUR_OFFER_SEARCH_UPDATE = "TOUR_OFFER_SEARCH_UPDATE",
  TOUR_OFFER_SEARCH_DELETE = "TOUR_OFFER_SEARCH_DELETE",

  CONN_SF_TOUR_UPDATE = "CONN_SF_TOUR_UPDATE",

  GDPR_REMOVAL = "GDPR_REMOVAL",

  ARI_RATES_UPDATE = "ARI_RATES_UPDATE",
  ARI_INVENTORY_UPDATE = "ARI_INVENTORY_UPDATE",
  ARI_AVAILABILITY_UPDATE = "ARI_AVAILABILITY_UPDATE",

  BEDBANK_GRANULAR_UPDATE = "BEDBANK_GRANULAR_UPDATE",
  BEDBANK_PROPERTY_FLIGHT_UPDATE = "BEDBANK_PROPERTY_FLIGHT_UPDATE",
  BEDBANK_SYNC = "BEDBANK_SYNC",
  BEDBANK_UPDATE = "BEDBANK_UPDATE",

  CRUISE_CACHE_SYNC = "CRUISE_CACHE_SYNC",
  CRUISE_UPDATE = "CRUISE_UPDATE",
  CRUISE_VENDOR_UPDATE = "CRUISE_VENDOR_UPDATE",
  CRUISE_PRICE_UPDATE = "CRUISE_PRICE_UPDATE",
  CRUISE_SAILING_DELETE = "CRUISE_SAILING_DELETE",
  CRUISE_ACTIVE_SAILINGS = "CRUISE_ACTIVE_SAILINGS",

  USER_SIGN_UP = "USER_SIGN_UP",

  CAR_HIRE_LOCATION_SYNC = "CAR_HIRE_LOCATION_SYNC",

  AD_FEED_BLOCK = "AD_FEED_BLOCK",

  AGENT_HUB_COMMISSION_RULES_SYNC = "AGENT_HUB_COMMISSION_RULES_SYNC",

  ACCOMM_PROPERTY_UPDATE = "ACCOMM_PROPERTY_UPDATE",
  ACCOMM_PROPERTY_DELETE = "ACCOMM_PROPERTY_DELETE",
}

export enum AnalyticEvents {
  PRODUCT_IMPRESSION = "product_impression",
  PRODUCT_CLICK = "product_click",
  PRODUCT_PURCHASE = "purchase_success",
}

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
    sessionToken,
    accessKeyId,
    secretAccessKey,
  };

  const sns = new SNSClient({ region, credentials });

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
  }: Message): Promise<void | PublishCommandOutput> {
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
        DataType: "String",
        StringValue: id,
      };
    }

    if (json) {
      try {
        messageAttributes.json = {
          DataType: "String",
          StringValue: encodeJson<unknown>(json),
        };
      } catch (e) {
        throw new InvalidEventJsonError("event json is invalid");
      }
    }

    const eventParams: PublishCommandInput = {
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

    const command = new PublishCommand(eventParams);

    return sns.send(command);
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
    sessionToken,
    accessKeyId,
    secretAccessKey,
  };

  const sqs = new SQSClient({ region, credentials });

  function deleteMessage(message: SQSMessage): () => Promise<void> {
    if (!message.ReceiptHandle) {
      throw new InvalidEventMessageError("invalid ReceiptHandle");
    }

    return async function ack(): Promise<void> {
      const input: DeleteMessageCommandInput = {
        QueueUrl: queueUrl,
        ReceiptHandle: message.ReceiptHandle,
      };

      const command = new DeleteMessageCommand(input);

      await sqs.send(command);
    };
  }

  function getAttributes(body: SQSMessage["Body"]): Message {
    if (!body) {
      return {
        type: "",
        source: "",
        checksum: 0,
        message: "",
      };
    }

    const bodyJson = JSON.parse(body);
    // handle s3 upload event
    // currently we can only one record per s3 event
    // https =//stackoverflow.com/questions/40765699/how-many-records-can-be-in-s3-put-event-lambda-trigger/40767563#40767563
    if (bodyJson.Records) {
      return bodyJson.Records[0];
    }

    if (bodyJson.MessageAttributes) {
      // handle sns message
      return mapAttributes(bodyJson);
    }

    // do nothing if the message type it not what we need
    return {
      type: "",
      source: "",
      checksum: 0,
      message: "",
    };
  }

  function mapAttributes(data: {
    Message: string;
    MessageAttributes: { [key: string]: { Value: string } };
  }): Message {
    const message: Message = {
      type: data.MessageAttributes.type.Value,
      source: data.MessageAttributes.source.Value,
      checksum: Number(data.MessageAttributes.checksum.Value),
      message: data.Message,
    };

    if (data.MessageAttributes.id) {
      message.id = data.MessageAttributes.id.Value;
    }

    if (data.MessageAttributes.uri) {
      message.uri = data.MessageAttributes.uri.Value;
    }

    if (data.MessageAttributes.json) {
      message.json = decodeJson(data.MessageAttributes.json.Value);
    }

    if (data.MessageAttributes.transactionId) {
      message.transactionId = data.MessageAttributes.transactionId.Value;
    }

    if (data.MessageAttributes.groupId) {
      message.groupId = data.MessageAttributes.groupId.Value;
    }

    return message;
  }

  function receiveMessages(
    processMessage: ProcessMessage,
    data: ReceiveMessageCommandOutput
  ): Promise<Message>[] {
    if (!data.Messages || data.Messages.length === 0) {
      return [];
    }

    return data.Messages.map(async (message: SQSMessage): Promise<Message> => {
      const attributes = getAttributes(message.Body);

      await processMessage(attributes, deleteMessage(message));

      return attributes;
    });
  }

  async function wait(
    processMessage: ProcessMessage,
    receiveMessageParams: ReceiveMessageParams
  ): Promise<Message[]> {
    const command = new ReceiveMessageCommand(receiveMessageParams);

    const data = await sqs.send(command);

    const messages = await Promise.all(receiveMessages(processMessage, data));

    return messages;
  }

  async function _poll(
    processMessage: ProcessMessage,
    n: number,
    t: number,
    receiveMessageParams: ReceiveMessageParams
  ): Promise<void> {
    if (t >= n) {
      return;
    }

    const results = await wait(processMessage, receiveMessageParams);

    if (results.length == 0) {
      return;
    }

    const next = await _poll(processMessage, n, t + 1, receiveMessageParams);

    return next;
  }

  function poll(processMessage: ProcessMessage, params?: PollParams) {
    return _poll(
      processMessage,
      params?.maxIterations ?? DEFAULT_MAX_ITERATIONS,
      0,
      {
        QueueUrl: queueUrl,
        MaxNumberOfMessages:
          params?.maxNumberOfMessages ?? DEFAULT_MAX_NUMBER_OF_MESSAGES,
      }
    );
  }

  return {
    poll,
    getAttributes,
    mapAttributes,
  };
}

export * as pubsub from "./gcp/pubsub";
