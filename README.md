# lib-events

SNS messages and SQS queues helper lib

## Dispatch

```js
const { dispatch, ORDER_CREATED } = require('lib-events');

dispatch({
  type: ORDER_CREATED,
  uri: `/api/orders/${order.id_orders}`,
  checksum: order.checksum,
  source: process.env.HEROKU_APP_NAME,
  message: `${user.fullname} just purchased ${order.offer.name}`
})
```

## Poll

```js
const { poll, ORDER_CREATED } = require('lib-events');

async function processMessage({ type, source, id, checksum }, ack) {
  if (type === ORDER_CREATED) {
    console.log(`${source} created an order!`);
  }

  await ack()
}

exports.process = async function () {
  await poll(processMessage, {
    maxNumberOfMessages: 10,
    maxIterations: 10
  });
}
```

## Running tests

```
yarn test
```

## Release

```
yarn publish
```
