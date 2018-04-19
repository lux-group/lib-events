const AWS = require("aws-sdk");

const sns = new AWS.SNS({
  apiVersion: "2010-03-31",
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_SNS_REGION
});

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

class InvalidEventMessageError extends Error {
  constructor(message, status) {
    super(message);
    this.name = this.constructor.name;
    Error.captureStackTrace(this, this.constructor);
  }
}

function dispatch({ type, uri, id, checksum, source, message }) {
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

  const eventParams = {
    MessageAttributes: messageAttributes,
    TopicArn: process.env.AWS_SNS_TOPIC_ARN,
    Message: message
  };

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
  const data = JSON.parse(body);

  const attributeReducer = (accumulator, currentValue) => {
    if (data.MessageAttributes[currentValue]) {
      accumulator[currentValue] = data.MessageAttributes[currentValue].Value;
    }
    return accumulator;
  };

  const attributeList = ["type", "uri", "id", "checksum"];

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
    InvalidEventTypeError,
    InvalidEventChecksumError,
    InvalidEventSourceError,
    InvalidEventMessageError
  },
  types
);
