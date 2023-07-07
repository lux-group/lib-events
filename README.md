# lib-events

`lib-events` is a helper to dispatch events using [AWS SNS](https://docs.aws.amazon.com/sns/latest/dg/welcome.html) and to listen to these events using [AWS SQS](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/welcome.html).

For a complete guide on how to set it up, please refer to: [Publishing and Consuming events](https://aussiecommerce.atlassian.net/wiki/spaces/TEC/pages/1799487497/Publishing+and+Consuming+events).

## Installation

Install `lib-events` via Luxury Escapes' NPM package registry by adding the following under `dependencies` in `package.json`:

```json
  "dependencies": {
    ...
    "@luxuryescapes/lib-events": "^3.0.4",
    ...
  }
```

## Creating the Publisher

The way we "dispatch" an event is by publishing it to Amazon SNS.<br />
First of all, you need to create a publisher, it will be your connection to SNS:

```js
import { createPublisher } from 'lib-events';

const publisher = createPublisher({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: 'ap-southeast-2',
  topic: 'arn:aws:sns:ap-southeast-2:1234:my-sns-topic',
  apiHost: 'https://our-api.com'
})
```

The `createPublisher` method allows you to pass one more parameter called `sessionToken`, you have to inform it if you are running it in dev or if you are assuming an IAM role.

To get temporary `accessKeyId`, `secretAccessKey` and `sessionToken` for your account (recommended), follow this guide: [Authenticate access using MFA through the AWS CLI](https://aws.amazon.com/premiumsupport/knowledge-center/authenticate-mfa-cli/).

Below is an example using an IAM role:

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

## Dispatching Events

Now that you created the publisher, you can use its instance to dispatch events:

```js
import { createPublisher, Events } from 'lib-events';

const publisher = createPublisher({ ... })

publisher.dispatch({
  type: Events.ORDER_CREATED,
  uri: `/api/orders/${order.id_orders}`,
  checksum: order.checksum,
  source: process.env.HEROKU_APP_NAME,
  message: `${user.fullname} just purchased ${order.offer.name}`
})
```

### Publishing to a fifo queue

```js
const publisher = createPublisher({
  ...,
  topic: 'arn:aws:sns:ap-southeast-2:1234:my-fifo-topic.fifo'
})

publisher.dispatch({
  type: Events.ORDER_CREATED,
  uri: `/api/orders/${order.id_orders}`,
  checksum: order.checksum,
  source: process.env.HEROKU_APP_NAME,
  message: `${user.fullname} just purchased ${order.offer.name}`,
  transactionId: '123456', // this is used for deduplication, required for fifo queues
  groupId: '123'// this is used for partitioning, required for fifo queues
})
```

## Creating the Consumer

We "listen" to events by consuming an Amazon SQS queue.<br />
To do that, create a consumer:

```js
import { createConsumer } from 'lib-events';

const consumer = createConsumer({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: 'ap-southeast-2',
  queueUrl: 'https://sqs.ap-southeast-2.amazonaws.com/1234/my-sqs-name'
})
```

As in [Creating the Publisher](#creating-the-publisher), you might need to inform `sessionToken` depending on your account type.

## Listening to Events

With the consumer instance, you can now listen to the SQS events.<br />
The common way to do that, is by running a worker process to constantly poll for messages in the consumer and process them accordingly:

```js
import { createConsumer, Events } from 'lib-events';

const consumer = createConsumer({ ... })

async function processMessages({ type, source, id, checksum }, ack) {
  switch (type) {
    case (Events.ORDER_CREATED):
      console.log(`${source} created an order!`);
      break;
    default:
      break;
  }

  await ack()
}

exports.process = async function () {
  await consumer.poll(processMessages, {
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

