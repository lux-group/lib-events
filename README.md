# lib-events

SNS messages and SQS queues helper lib

## Publisher

A small wrapper around SNS

```js
const { createPublisher } = require('lib-events');

const publisher = createPublisher({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: 'ap-southeast-2',
  topic: 'arn:aws:sns:ap-southeast-2:1234:my-sns-topic',
  apiHost: 'https://our-api.com'
})
```

If you are assuming an IAM role you can pass the `sessionToken` parameter:

```js
const role = getRoleCredentials();

const publisher = createPublisher({
  accessKeyId: role.Credentials.AccessKeyId,
  secretAccessKey: role.Credentials.SecretAccessKey,
  sessionToken: role.Credentials.SessionToken,
  region: 'ap-southeast-2',
  topic: 'arn:aws:sns:ap-southeast-2:1234:my-sns-topic',
  apiHost: 'https://our-api.com'
})
```


### Dispatch

```js
const { createPublisher, ORDER_CREATED } = require('lib-events');

const publisher = createPublisher({ ... })

publisher.dispatch({
  type: ORDER_CREATED,
  uri: `/api/orders/${order.id_orders}`,
  checksum: order.checksum,
  source: process.env.HEROKU_APP_NAME,
  message: `${user.fullname} just purchased ${order.offer.name}`
})
```

#### Publishing to a fifo queue

```js
const publisher = createPublisher({ ..., topic: 'arn:aws:sns:ap-southeast-2:1234:my-fifo-topic.fifo' })

publisher.dispatch({
  type: ORDER_CREATED,
  uri: `/api/orders/${order.id_orders}`,
  checksum: order.checksum,
  source: process.env.HEROKU_APP_NAME,
  message: `${user.fullname} just purchased ${order.offer.name}`,
  transactionId: '123456', // this is used for deduplication, required for fifo queues
  groupId: '123'// this is used for partitioning, required for fifo queues
})
```

## Consumers

A small wrapper around SQS

```js
const { createConsumer } = require('lib-events');

const consumer = createConsumer({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: 'ap-southeast-2',
  queueUrl: 'https://sqs.ap-southeast-2.amazonaws.com/1234/my-sqs-name'
})
```

### Poll

```js
const { createConsumer, ORDER_CREATED } = require('lib-events');

const consumer = createConsumer({ ... })

async function processMessage({ type, source, id, checksum }, ack) {
  if (type === ORDER_CREATED) {
    console.log(`${source} created an order!`);
  }

  await ack()
}

exports.process = async function () {
  await consumer.poll(processMessage, {
    maxNumberOfMessages: 10,
    maxIterations: 10
  });
}
```

## Running tests

```
yarn test
```

## Publishing

Update the version in package.json as part of your PR and CircleCI will do the rest.
