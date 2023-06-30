import { createConsumer, createPublisher, Events, Message } from "./index";

const accessKeyId = process.env.AWS_ACCESS_KEY_ID || ''

const secretAccessKey = process.env.AWS_SECRET_ACCESS_KEY || ''

const region = process.env.AWS_SQS_REGION || ''

const consumer = createConsumer({
  accessKeyId,
  secretAccessKey,
  region,
  queueUrl: process.env.AWS_SQS_QUERY_URL || ''
});

const publisher = createPublisher({
  accessKeyId,
  secretAccessKey,
  region,
  topic: process.env.AWS_SNS_TOPIC_ARN || '',
  apiHost: 'http://localhost'
});

async function processMessage(message: Message, ack: () => Promise<void>): Promise<void> {
  console.log(message)
  await ack();
}

async function wait(duration: number): Promise<void> {
  await new Promise(resolve => {
    setTimeout(resolve, duration);
  });
}

const run = async (): Promise<void> => {
  console.log("dispatch event")

  await publisher.dispatch({
    type: Events.OFFER_UPDATE,
    uri: `/api/orders/1`,
    checksum: 0,
    source: 'lib-events-integration-test',
    message: 'test',
    json: {
      test: true,
      value: 5
    }
  });

  console.log("done.")

  await wait(1000);

  console.log("poll for events")

  await consumer.poll(processMessage, {
    maxNumberOfMessages: 10,
    maxIterations: 10,
  });

  console.log("done.")
};

run()
  .then(() => {
    process.exit(0);
  })
