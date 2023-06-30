import {
  createPublisher,
  createConsumer,
  InvalidEventTypeError,
  InvalidFIFOMessageError,
  Events,
} from './index';

import { encodeJson } from './base64';

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

describe("index", () => {
  describe("poll", () => {
    it('should have queue poll fun', function() {
      expect(consumer.poll).toBeDefined()
    });
  })

  describe("dispatch", () => {
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

    it('should throw error if no transactionId for fifo message', function() {
      const fun = () => {
        publisherFIFO.dispatch({
          type: Events.ORDER_PENDING,
          uri: '/api',
          checksum: 1,
          source: 'test',
          message: 'test',
          groupId: '123',
        })
      }

      const error = new InvalidFIFOMessageError('transactionId is required for FIFO messages');

      expect(fun).toThrow(error)
    });

    it('should throw error if no groupId for fifo message', function() {
      const fun = () => {
        publisherFIFO.dispatch({
          type: Events.ORDER_PENDING,
          uri: '/api',
          checksum: 1,
          source: 'test',
          message: 'test',
          transactionId: '123'
        })
      }

      const error = new InvalidFIFOMessageError('groupId is required for FIFO messages');

      expect(fun).toThrow(error)
    });
  });

  describe("attributes", () => {
    it('should pluck the message attributes', function() {
      const attributes = {
        source: 'service',
        message: 'message',
        type: 'ORDERS_CHECKSUM',
        checksum: 1
      }

      const body = JSON.stringify({
        Message: 'message',
        MessageAttributes: {
          source: {
            Value: 'service'
          },
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

    it('should get json', function() {
      const attributes = {
        source: 'service',
        message: 'message',
        type: 'ORDERS_CHECKSUM',
        checksum: 1,
        json: {"name":"Beckie","age":28,"location":{"country": "Finland"}}
      }

      const body = JSON.stringify({
        Message: 'message',
        MessageAttributes: {
          source: {
            Value: 'service'
          },
          type: {
            Value: 'ORDERS_CHECKSUM'
          },
          checksum: {
            Value: 1
          },
          json: {
            Value: encodeJson({"name":"Beckie","age":28,"location":{"country": "Finland"}})
          }
        }
      });

      expect(consumer.getAttributes(body)).toEqual(attributes)
    });

    it('should pluck the message attributes without id', function() {
      const attributes = {
        source: 'service',
        message: 'message',
        type: 'ORDERS_CHECKSUM',
        checksum: 1
      }

      const body = JSON.stringify({
        Message: 'message',
        MessageAttributes: {
          source: {
            Value: 'service'
          },
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
  });
});
