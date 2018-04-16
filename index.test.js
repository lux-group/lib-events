const assert = require('assert');
const {
  dispatch,
  poll,
  getAttributes,
  InvalidEventTypeError,
	InvalidEventChecksumError,
	InvalidEventSourceError,
  InvalidEventMessageError,
  ORDERS_CHECKSUM
} = require('./index.js');

it('should have queue poll fun', function() {
  expect(poll).toBeDefined()
});

it('should throw error if invalid type', function() {
  const fun = () => {
    dispatch({
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
    dispatch({
      type: ORDERS_CHECKSUM,
      uri: '/api',
      checksum: 'x',
      source: 'test',
      message: 'test'
    })
  }

  const error = new InvalidEventChecksumError('checksum is not a number');

  expect(fun).toThrow(error)
});

it('should throw error no source', function() {
  const fun = () => {
    dispatch({
      type: ORDERS_CHECKSUM,
      uri: '/api',
      checksum: 1,
      source: null,
      message: 'test'
    })
  }

  const error = new InvalidEventSourceError('event source is required');

  expect(fun).toThrow(error)
});

it('should throw error no source', function() {
  const fun = () => {
    dispatch({
      type: ORDERS_CHECKSUM,
      uri: '/api',
      checksum: 1,
      source: 'test',
      message: null
    })
  }

  const error = new InvalidEventMessageError('event message is required');

  expect(fun).toThrow(error)
});

it('should pluck the message attributes', function() {
  const attributes = {
    type: 'ORDERS_CHECKSUM',
    uri: 'http://api.luxgroup.com',
    id: 'abcd',
    checksum: 1
  }

  const body = JSON.stringify({
    MessageAttributes: {
      type: {
        Value: 'ORDERS_CHECKSUM'
      },
      uri: {
        Value: 'http://api.luxgroup.com'
      },
      id: {
        Value: 'abcd'
      },
      checksum: {
        Value: 1
      }
    }
  });

  expect(getAttributes(body)).toEqual(attributes)
});

it('should pluck the message attributes without id', function() {
  const attributes = {
    type: 'ORDERS_CHECKSUM',
    uri: 'http://api.luxgroup.com',
    checksum: 1
  }

  const body = JSON.stringify({
    MessageAttributes: {
      type: {
        Value: 'ORDERS_CHECKSUM'
      },
      uri: {
        Value: 'http://api.luxgroup.com'
      },
      checksum: {
        Value: 1
      }
    }
  });

  expect(getAttributes(body)).toEqual(attributes)
});
