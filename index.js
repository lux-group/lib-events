const AWS = require("aws-sdk");
const { encodeJson, decodeJson } = require("./base64");

// Amazon SNS currently allows a maximum size of 256 KB for published messages.
const MAX_EVENT_MESSAGE_SIZE = 256 * 1024;

const typeList = [
  "ORDER_PENDING",
  "ORDER_COMPLETED",
  "ORDER_CANCELLED",
  "ORDER_REFUNDED",
  "ORDER_ABANDONED",
  "ORDER_AWAITING_PAYMENT",
  "ORDER_AWAITING_PURCHASE",
  "ORDER_NEEDS_ATTENTION",
  "ORDER_PAYMENT_FAILED",
  "ORDER_TOUCH",

  "ORDERS_CHECKSUM",
  "ORDERS_CHECKSUM_ERROR",

  "ORDER_ITEM_CREATED",
  "ORDER_ITEM_COMPLETED",
  "ORDER_ITEM_AWAITING_DATES",
  "ORDER_ITEM_FAILED",
  "ORDER_ITEM_CANCELLED",
  "ORDER_ITEM_TOUCH",
  "ORDER_ITEM_CHANGE_DATES",
  "ORDER_ITEM_DELETE_RESERVATION",
  "ORDER_ITEM_UPDATE_RESERVATION",
  "ORDER_ITEM_REFUND",
  "ORDER_ITEMS_CHECKSUM",
  "ORDER_ITEMS_CHECKSUM_ERROR",

  "ORDER_ADDON_ITEM_CANCELLED",

  "ORDER_FLIGHT_ITEM_CREATED",
  "ORDER_FLIGHT_ITEM_COMPLETED",
  "ORDER_FLIGHT_ITEM_FAILED",
  "ORDER_FLIGHT_ITEM_CANCELLED",

  "OFFER_UPDATE",

  "RATE_PLAN_UPDATE",
  "RATE_PLAN_DELETE",

  "PROPERTY_UPDATE",
  "PROPERTY_DELETE",

  "PROPERTY_RATING_UPDATE",

  "ROOM_AVAILABILITY_UPDATE",
  "RATE_AVAILABILITY_UPDATE",

  "HOTEL_RESERVATION_SITEMINDER_ERROR",
  "HOTEL_RESERVATION_TRAVELCLICK_ERROR",

  "RESERVATION_FX_RATES_UPDATE",

  "SITEMINDER_CURRENCY_ERROR",

  "VOUCHER_UPDATE",

  "TOUR_OFFER_UPDATE",
  "TOUR_UPDATE",
  "TOUR_DELETE",

  "CONN_SF_TOUR_UPDATE",

  "GDPR_REMOVAL",

  "ARI_RATES_UPDATE",
  "ARI_INVENTORY_UPDATE",
  "ARI_AVAILABILITY_UPDATE",

  "BEDBANK_PROPERTY_FLIGHT_UPDATE",
  "BEDBANK_SYNC",
  "BEDBANK_UPDATE",

  "CRUISE_SYNC",
  "CRUISE_UPDATE",

  "USER_SIGN_UP",
];

const typeReducer = (accumulator, currentValue) => {
  accumulator[currentValue] = currentValue;
  return accumulator;
};

const types = typeList.reduce(typeReducer, {});

class InvalidEventTypeError extends Error {
  constructor(message, status) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

class InvalidEventChecksumError extends Error {
  constructor(message, status) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

class InvalidEventSourceError extends Error {
  constructor(message, status) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

class InvalidEventMessageError extends Error {
  constructor(message, status) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

class InvalidEventJsonError extends Error {
  constructor(message, status) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

class InvalidEventSizeError extends Error {
  constructor(message, status) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

class InvalidFIFOMessageError extends Error {
  constructor(message, status) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

function createPublisher({
  accessKeyId,
  secretAccessKey,
  sessionToken,
  region,
  topic,
  apiHost,
}) {
  const credentials = {
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
  }) {
    if (!types[type]) {
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

    const messageAttributes = {
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
          StringValue: encodeJson(json),
        };
      } catch (e) {
        throw new InvalidEventJsonError("event json is invalid");
      }
    }

    const eventParams = {
      MessageAttributes: messageAttributes,
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

function createConsumer({
  accessKeyId,
  secretAccessKey,
  sessionToken,
  region,
  queueUrl,
}) {
  const credentials = {
    apiVersion: "2012-11-05",
    accessKeyId,
    secretAccessKey,
    region,
  };

  if (sessionToken) {
    credentials.sessionToken = sessionToken;
  }
  const sqs = new AWS.SQS(credentials);

  function deleteMessage(message) {
    return function ack() {
      return new Promise((accept, reject) => {
        sqs.deleteMessage(
          {
            QueueUrl: queueUrl,
            ReceiptHandle: message.ReceiptHandle,
          },
          (err, response) => {
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

  function getAttributes(body) {
    const bodyJson = JSON.parse(body);
    // handle s3 upload event
    // currently we can only one record per s3 event
    // https://stackoverflow.com/questions/40765699/how-many-records-can-be-in-s3-put-event-lambda-trigger/40767563#40767563
    if (bodyJson.Records) return bodyJson.Records[0];
    else if (bodyJson.MessageAttributes) {
      // handle sns message
      return mapAttributes(bodyJson);
    }

    // do nothing if the message type it not what we need
    return {};
  }

  function mapAttributes(data) {
    const attributeReducer = (accumulator, currentValue) => {
      if (data.MessageAttributes[currentValue]) {
        if (currentValue === "json") {
          try {
            accumulator[currentValue] = decodeJson(
              data.MessageAttributes[currentValue].Value
            );
          } catch (e) {
            console.log(e);
          }
        } else {
          accumulator[currentValue] =
            data.MessageAttributes[currentValue].Value;
        }
      }
      return accumulator;
    };

    const attributeList = ["type", "uri", "id", "checksum", "source", "json"];

    return attributeList.reduce(attributeReducer, {});
  }

  function receiveMessages(processMessage, data) {
    if (!data.Messages || data.Messages.length === 0) {
      return [];
    }
    return data.Messages.map((message) => {
      return processMessage(
        getAttributes(message.Body),
        deleteMessage(message)
      );
    });
  }

  function wait(processMessage, receiveMessageParams) {
    return new Promise((accept, reject) => {
      sqs.receiveMessage(receiveMessageParams, (err, data) => {
        if (err) {
          return reject(err);
        }
        Promise.all(receiveMessages(processMessage, data))
          .then(accept)
          .catch(reject);
      });
    });
  }

  function pollStart(processMessage, n, t, receiveMessageParams) {
    if (t >= n) {
      return Promise.resolve();
    }
    return wait(processMessage, receiveMessageParams).then((results) => {
      if (results.length == 0) {
        return Promise.resolve();
      }
      return poll(processMessage, n, t + 1);
    });
  }

  function poll(
    processMessage,
    { maxNumberOfMessages, maxIterations } = {
      maxNumberOfMessages: 10,
      maxIterations: 10,
    }
  ) {
    const receiveMessageParams = {
      QueueUrl: queueUrl,
      MaxNumberOfMessages: maxNumberOfMessages,
    };

    return pollStart(processMessage, maxIterations, 0, receiveMessageParams);
  }

  return {
    poll,
    getAttributes,
    mapAttributes,
  };
}

module.exports = Object.assign(
  {
    createPublisher,
    createConsumer,
    InvalidEventTypeError,
    InvalidEventChecksumError,
    InvalidEventSourceError,
    InvalidEventMessageError,
    InvalidFIFOMessageError,
  },
  types
);
