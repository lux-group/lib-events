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

async function processMessage({ type, source }, ack) {
  if (type === ORDER_CREATED) {
    console.log(`${source} created an order!`);
  }

  await ack()
}

exports.process = async function () {
  await poll(processMessage);
}
```

## Running tests

```
yarn test
```

## Release

Use `npm` to patch, minor or whatever version:

```
npm version patch -m "release version %s"
git push && git push --tags
```

https://docs.npmjs.com/cli/version
