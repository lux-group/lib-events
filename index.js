const AWS = require("aws-sdk");

const sns = new AWS.SNS({
  apiVersion: "2010-03-31",
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_SNS_REGION
});

const typeList = [
  "ORDER_CREATED",
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
  "ORDER_ITEM_CREATED",
  "ORDER_ITEM_COMPLETED",
  "ORDER_ITEM_AWAITING_DATES",
  "ORDER_ITEM_FAILED",
  "ORDER_ITEM_CANCELLED",
  "ORDER_ITEM_TOUCH",

  "ORDER_ITEMS_CHECKSUM",
  "ORDER_ITEMS_CHECKSUM_ERROR"
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

function dispatch({ type, uri, checksum, source, message }) {
  if (process.env.IGNORE_EVENTS == "true") {
    return Promise.resolve();
  }

  if (!types[type]) {
    throw new InvalidEventTypeError(`invalid event type '${type}'`);
  }

  if (isNaN(checksum)) {
    throw new InvalidEventChecksumError("checksum is not a number");
  }

  if (!source) {
    throw new InvalidEventSourceError("event source is required");
  }

  const eventParams = {
    MessageAttributes: {
      uri: {
        DataType: "String",
        StringValue: `${process.env.API_HOST}${uri}`
      },
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
    },
    TopicArn: process.env.AWS_SNS_TOPIC_ARN,
    Message: message
  };

  return sns.publish(eventParams).promise();
}

const sqs = new AWS.SQS({
  apiVersion: "2012-11-05",
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_SQS_REGION
});

const queueUrl = process.env.AWS_SQS_QUERY_URL;

const receiveMessageParams = {
  QueueUrl: queueUrl,
  MaxNumberOfMessages: 10
};

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
  const data = JSON.parse(body);
  const { type, uri } = data.MessageAttributes;

  return {
    type: type.Value,
    uri: uri.Value
  };
}

function receiveMessages(processMessage, data) {
  if (!data.Messages || data.Messages.length === 0) {
    return [];
  }
  return data.Messages.map(message => {
    return processMessage(getAttributes(message.Body), deleteMessage(message));
  });
}

function wait(processMessage) {
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

function poll(processMessage, n = 10, t = 1) {
  if (t >= n) {
    return Promise.resolve();
  }
  return wait(processMessage).then(results => {
    if (results.length == 0) {
      return Promise.resolve();
    }
    return poll(processMessage, n, t + 1);
  });
}

module.exports = Object.assign(
  {
    poll,
    dispatch,
    InvalidEventTypeError,
    InvalidEventChecksumError,
    InvalidEventSourceError
  },
  types
);
