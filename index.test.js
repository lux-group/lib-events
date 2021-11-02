const assert = require('assert');
const {
  createPublisher,
  createConsumer,
  getAttributes,
  InvalidEventTypeError,
	InvalidEventChecksumError,
	InvalidEventSourceError,
  InvalidEventMessageError,
  InvalidFifoMessageError,
  ORDER_PENDING,
  ORDERS_CHECKSUM
} = require('./index.js');

const publisher = createPublisher({
  accessKeyId: 'key',
  secretAccessKey: 'secret',
  region: 'ap-southeast-2',
  topic: 'my-sns-topic',
  apiHost: 'https://our-api.com'
})

const publisherFIFO = createPublisher({
  accessKeyId: 'key',
  secretAccessKey: 'secret',
  region: 'ap-southeast-2',
  topic: 'my-sns-topic.fifo',
  apiHost: 'https://our-api.com'
})

const consumer = createConsumer({
  accessKeyId: 'key',
  secretAccessKey: 'secret',
  region: 'ap-southeast-2',
  queueUrl: 'https://sqs.ap-southeast-2.amazonaws.com/1234/my-sqs-name'
})

it('should have queue poll fun', function() {
  expect(consumer.poll).toBeDefined()
});

it('should throw error if invalid type', function() {
  const fun = () => {
    publisher.dispatch({
      type: 'NA',
      uri: '/api',
      checksum: 1,
      source: 'test',
      message: 'test'
    })
  }

  const error = new InvalidEventTypeError("invalid event type 'NA'");

  expect(fun).toThrow(error)
});

it('should throw error if invalid checksum', function() {
  const fun = () => {
    publisher.dispatch({
      type: ORDER_PENDING,
      uri: '/api',
      checksum: 'x',
      source: 'test',
      message: 'test'
    })
  }

  const error = new InvalidEventChecksumError('checksum is not a number');

  expect(fun).toThrow(error)
});

it('should throw error if no source', function() {
  const fun = () => {
    publisher.dispatch({
      type: ORDER_PENDING,
      uri: '/api',
      checksum: 1,
      source: null,
      message: 'test'
    })
  }

  const error = new InvalidEventSourceError('event source is required');

  expect(fun).toThrow(error)
});

it('should throw error if no message', function() {
  const fun = () => {
    publisher.dispatch({
      type: ORDER_PENDING,
      uri: '/api',
      checksum: 1,
      source: 'test',
      message: null
    })
  }

  const error = new InvalidEventMessageError('event message is required');

  expect(fun).toThrow(error)
});

it('should throw error if no transactionId for fifo message', function() {
  const fun = () => {
    publisherFIFO.dispatch({
      type: ORDER_PENDING,
      uri: '/api',
      checksum: 1,
      source: 'test',
      message: 'test',
      groupId: '123',
      transactionId: null
    })
  }

  const error = new InvalidFifoMessageError('transactionId is required for FIFO messages');

  expect(fun).toThrow(error)
});

it('should throw error if no groupId for fifo message', function() {
  const fun = () => {
    publisherFIFO.dispatch({
      type: ORDER_PENDING,
      uri: '/api',
      checksum: 1,
      source: 'test',
      message: 'test',
      groupId: null,
      transactionId: '123'
    })
  }

  const error = new InvalidFifoMessageError('groupId is required for FIFO messages');

  expect(fun).toThrow(error)
});

it('should pluck the message attributes', function() {
  const attributes = {
    type: 'ORDERS_CHECKSUM',
    checksum: 1
  }

  const body = JSON.stringify({
    MessageAttributes: {
      type: {
        Value: 'ORDERS_CHECKSUM'
      },
      checksum: {
        Value: 1
      }
    }
  });

  expect(consumer.getAttributes(body)).toEqual(attributes)
});

it('should pluck the message attributes without id', function() {
  const attributes = {
    type: 'ORDERS_CHECKSUM',
    checksum: 1
  }

  const body = JSON.stringify({
    MessageAttributes: {
      type: {
        Value: 'ORDERS_CHECKSUM'
      },
      checksum: {
        Value: 1
      }
    }
  });

  expect(consumer.getAttributes(body)).toEqual(attributes)
});
