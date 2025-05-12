import { createConsumer } from "./index";

import { encodeJson } from "./base64";

const consumer = createConsumer({
  accessKeyId: "key",
  secretAccessKey: "secret",
  region: "ap-southeast-2",
  queueUrl: "https://sqs.ap-southeast-2.amazonaws.com/1234/my-sqs-name",
});

describe("index", () => {
  describe("poll", () => {
    it("should have queue poll fun", function () {
      expect(consumer.poll).toBeDefined();
    });
  });

  describe("attributes", () => {
    it("should pluck the message attributes", function () {
      const attributes = {
        source: "service",
        message: "message",
        type: "ORDERS_CHECKSUM",
        checksum: 1,
      };

      const body = JSON.stringify({
        Message: "message",
        MessageAttributes: {
          source: {
            Value: "service",
          },
          type: {
            Value: "ORDERS_CHECKSUM",
          },
          checksum: {
            Value: 1,
          },
        },
      });

      expect(consumer.getAttributes(body)).toEqual(attributes);
    });

    it("should get json", function () {
      const attributes = {
        source: "service",
        message: "message",
        type: "ORDERS_CHECKSUM",
        checksum: 1,
        json: { name: "Beckie", age: 28, location: { country: "Finland" } },
      };

      const body = JSON.stringify({
        Message: "message",
        MessageAttributes: {
          source: {
            Value: "service",
          },
          type: {
            Value: "ORDERS_CHECKSUM",
          },
          checksum: {
            Value: 1,
          },
          json: {
            Value: encodeJson({
              name: "Beckie",
              age: 28,
              location: { country: "Finland" },
            }),
          },
        },
      });

      expect(consumer.getAttributes(body)).toEqual(attributes);
    });

    it("should pluck the message attributes without id", function () {
      const attributes = {
        source: "service",
        message: "message",
        type: "ORDERS_CHECKSUM",
        checksum: 1,
      };

      const body = JSON.stringify({
        Message: "message",
        MessageAttributes: {
          source: {
            Value: "service",
          },
          type: {
            Value: "ORDERS_CHECKSUM",
          },
          checksum: {
            Value: 1,
          },
        },
      });

      expect(consumer.getAttributes(body)).toEqual(attributes);
    });
  });
});
