import {
  PublishCommand,
  PublishCommandInput,
  PublishCommandOutput,
  SNSClient,
} from "@aws-sdk/client-sns";
import { Credentials } from "@aws-sdk/types";
import { encodeJson } from "../base64";
import {
  Events,
  InvalidEventChecksumError,
  InvalidEventJsonError,
  InvalidEventMessageError,
  InvalidEventSizeError,
  InvalidEventSourceError,
  InvalidEventTypeError,
  InvalidFIFOMessageError,
  Message,
} from "../index";

type PublisherParams = {
  accessKeyId?: string;
  secretAccessKey?: string;
  sessionToken?: string;
  region: string;
  topic: string;
  apiHost: string;
};

export interface Publisher {
  dispatch: (message: Message) => Promise<void | PublishCommandOutput>;
}

type MessageAttribute = {
  DataType: string;
  StringValue: string;
};

type MessageAttributes = {
  type: MessageAttribute;
  checksum: MessageAttribute;
  source: MessageAttribute;
  uri?: MessageAttribute;
  id?: MessageAttribute;
  json?: MessageAttribute;
};

// Amazon SNS currently allows a maximum size of 256 KB for published messages.
const MAX_EVENT_MESSAGE_SIZE = 256 * 1024;

const createDispatchFunction = (
  apiHost: string,
  isFIFO: boolean,
  snsClient: SNSClient,
  topicArn: string
) => {
  return function dispatch({
    type,
    uri,
    id,
    checksum,
    source,
    message,
    json,
    groupId,
    transactionId,
  }: Message): Promise<void | PublishCommandOutput> {
    if (!Object.values<string>(Events).includes(type)) {
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

    if (isFIFO && !groupId) {
      throw new InvalidFIFOMessageError(
        "groupId is required for FIFO messages"
      );
    }

    if (isFIFO && !transactionId) {
      throw new InvalidFIFOMessageError(
        "transactionId is required for FIFO messages"
      );
    }

    const messageAttributes: MessageAttributes = {
      type: {
        DataType: "String",
        StringValue: type,
      },
      checksum: {
        DataType: "Number",
        StringValue: checksum.toString(),
      },
      source: {
        DataType: "String",
        StringValue: source,
      },
    };

    if (uri) {
      messageAttributes.uri = {
        DataType: "String",
        StringValue: `${apiHost}${uri}`,
      };
    }

    if (id) {
      messageAttributes.id = {
        DataType: "String",
        StringValue: id,
      };
    }

    if (json) {
      try {
        messageAttributes.json = {
          DataType: "String",
          StringValue: encodeJson<unknown>(json),
        };
      } catch (e) {
        throw new InvalidEventJsonError("event json is invalid");
      }
    }

    const eventParams: PublishCommandInput = {
      MessageAttributes: { ...messageAttributes },
      TopicArn: topicArn,
      Message: message,
    };

    if (
      Buffer.byteLength(JSON.stringify(eventParams), "utf8") >
      MAX_EVENT_MESSAGE_SIZE
    ) {
      throw new InvalidEventSizeError("json message exceeded limit of 256KB");
    }

    if (groupId) {
      eventParams.MessageGroupId = groupId;
    }

    if (transactionId) {
      eventParams.MessageDeduplicationId = transactionId;
    }

    if (process.env.IGNORE_EVENTS == "true") {
      return Promise.resolve();
    }

    const command = new PublishCommand(eventParams);

    return snsClient.send(command);
  };
};

export function createPublisher({
  accessKeyId,
  secretAccessKey,
  sessionToken,
  region,
  topic,
  apiHost,
}: PublisherParams): Publisher {
  const credentials =
    accessKeyId && secretAccessKey
      ? {
          credentials: {
            sessionToken,
            accessKeyId,
            secretAccessKey,
          },
        }
      : {};

  const snsClient = new SNSClient({ region, ...credentials });

  const isFIFO = topic.endsWith(".fifo");

  return {
    dispatch: createDispatchFunction(apiHost, isFIFO, snsClient, topic),
  };
}
