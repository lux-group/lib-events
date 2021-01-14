const AWS = require("aws-sdk");
const { encodeJson, decodeJson } = require("./base64");

const sns = new AWS.SNS({
  apiVersion: "2010-03-31",
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_SNS_REGION
});

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
  "ORDER_ITEMS_CHECKSUM",
  "ORDER_ITEMS_CHECKSUM_ERROR",

  "ORDER_ADDON_ITEM_CANCELLED",

  "ORDER_FLIGHT_ITEM_CREATED",
  "ORDER_FLIGHT_ITEM_COMPLETED",
  "ORDER_FLIGHT_ITEM_FAILED",
  "ORDER_FLIGHT_ITEM_CANCELLED",

  "OFFER_UPDATE",

  "PROPERTY_UPDATE",
  "PROPERTY_DELETE",

  "ROOM_AVAILABILITY_UPDATE",

  "HOTEL_RESERVATION_SITEMINDER_ERROR",
  "HOTEL_RESERVATION_TRAVELCLICK_ERROR",

  "RESERVATION_FX_RATE_UPDATE",

  "SITEMINDER_CURRENCY_ERROR",

  "VOUCHER_UPDATE",

  "TOUR_UPDATE",
  "TOUR_DELETE"
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

function dispatch({ type, uri, id, checksum, source, message, json }) {
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

  const messageAttributes = {
    type: {
      DataType: "String",
      StringValue: type
    },
    checksum: {
      DataType: "Number",
      StringValue: checksum.toString()
    },
    source: {
      DataType: "String",
      StringValue: source
    }
  };

  if (uri) {
    messageAttributes.uri = {
      DataType: "String",
      StringValue: `${process.env.API_HOST}${uri}`
    };
  }

  if (id) {
    messageAttributes.id = {
      DataType: "String",
      StringValue: id
    };
  }

  if (json) {
    try {
      messageAttributes.json = {
        DataType: "String",
        StringValue: encodeJson(json)
      };
    } catch (e) {
      throw new InvalidEventJsonError("event json is invalid");
    }
  }

  const eventParams = {
    MessageAttributes: messageAttributes,
    TopicArn: process.env.AWS_SNS_TOPIC_ARN,
    Message: message
  };

  if (
    Buffer.byteLength(JSON.stringify(eventParams), "utf8") >
    MAX_EVENT_MESSAGE_SIZE
  ) {
    throw new InvalidEventSizeError("json message exceeded limit of 256KB");
  }

  if (process.env.IGNORE_EVENTS == "true") {
    return Promise.resolve();
  }

  return sns.publish(eventParams).promise();
}

const sqs = new AWS.SQS({
  apiVersion: "2012-11-05",
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_SQS_REGION
});

const queueUrl = process.env.AWS_SQS_QUERY_URL;

function deleteMessage(message) {
  return function ack() {
    return new Promise((accept, reject) => {
      sqs.deleteMessage(
        {
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle
        },
        (err, response) => {
          if (err) {
            return reject(err);
          }
          return accept({
            response,
            message
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
        accumulator[currentValue] = data.MessageAttributes[currentValue].Value;
      }
    }
    return accumulator;
  };

  const attributeList = ["type", "uri", "id", "checksum", "json"];

  return attributeList.reduce(attributeReducer, {});
}

function receiveMessages(processMessage, data) {
  if (!data.Messages || data.Messages.length === 0) {
    return [];
  }
  return data.Messages.map(message => {
    return processMessage(getAttributes(message.Body), deleteMessage(message));
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
  return wait(processMessage, receiveMessageParams).then(results => {
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
    maxIterations: 10
  }
) {
  const receiveMessageParams = {
    QueueUrl: queueUrl,
    MaxNumberOfMessages: maxNumberOfMessages
  };

  return pollStart(processMessage, maxIterations, 0, receiveMessageParams);
}

module.exports = Object.assign(
  {
    poll,
    dispatch,
    getAttributes,
    mapAttributes,
    InvalidEventTypeError,
    InvalidEventChecksumError,
    InvalidEventSourceError,
    InvalidEventMessageError
  },
  types
);
