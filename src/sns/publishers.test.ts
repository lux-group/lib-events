import { createPublisher, Publisher } from "./publishers";
import { Events, InvalidEventTypeError, InvalidFIFOMessageError } from "../index";
import { SNSClient } from "@aws-sdk/client-sns";

jest.mock("@aws-sdk/client-sns");

describe("publishers", () => {

  describe("client creation", () => {
    let mockSns!: jest.MockedClass<typeof SNSClient>
    const commonCreateParams = {
      region: "ap-southeast-2",
      topic: "my-sns-topic",
      apiHost: "https://our-api.com",
    }

    beforeEach(() => {
      mockSns = SNSClient as jest.MockedClass<typeof SNSClient>
    })

    it("should use the credentials supplied when creating the SNS client", () => {
      createPublisher({
        accessKeyId: "key",
        secretAccessKey: "secret",
        ...commonCreateParams
      });

      expect(mockSns.prototype.constructor).toHaveBeenCalledWith(expect.objectContaining({
        region: "ap-southeast-2",
        credentials: {
          accessKeyId: "key",
          secretAccessKey: "secret"
        }
      }))
    });

    it("should use the session token when supplied", () => {
      createPublisher({
        accessKeyId: "key",
        secretAccessKey: "secret",
        sessionToken: "sessionToken",
        ...commonCreateParams
      });

      expect(mockSns.prototype.constructor).toHaveBeenCalledWith(expect.objectContaining({
        credentials: expect.objectContaining({sessionToken: "sessionToken"}),
      }))
    });

    it("should not pass the credentials when access key id is missing", () => {
      createPublisher({
        secretAccessKey: "secret",
        ...commonCreateParams
      });

      expect(mockSns.prototype.constructor).toHaveBeenCalledWith(expect.not.objectContaining({
        credentials: expect.anything()
      }))
    });

    it("should not pass the credentials when access key secret is missing", () => {
      createPublisher({
        accessKeyId: "key",
        ...commonCreateParams
      });

      expect(mockSns.prototype.constructor).toHaveBeenCalledWith(expect.not.objectContaining({
        credentials: expect.anything()
      }))
    });
  });

  describe("dispatcher", () => {
    let publisher!: Publisher
    let publisherFIFO!: Publisher

    beforeEach(() => {
      jest.resetAllMocks()

      publisher = createPublisher({
        accessKeyId: "key",
        secretAccessKey: "secret",
        region: "ap-southeast-2",
        topic: "my-sns-topic",
        apiHost: "https://our-api.com",
      });

      publisherFIFO = createPublisher({
        accessKeyId: "key",
        secretAccessKey: "secret",
        region: "ap-southeast-2",
        topic: "my-sns-topic.fifo",
        apiHost: "https://our-api.com",
      });

    });

    it("should throw error if invalid type", function () {
      const fun = () => {
        publisher.dispatch({
          type: "NA",
          uri: "/api",
          checksum: 1,
          source: "test",
          message: "test",
        });
      };

      const error = new InvalidEventTypeError("invalid event type 'NA'");

      expect(fun).toThrow(error);
    });

    it("should throw error if no transactionId for fifo message", function () {
      const fun = () => {
        publisherFIFO.dispatch({
          type: Events.ORDER_PENDING,
          uri: "/api",
          checksum: 1,
          source: "test",
          message: "test",
          groupId: "123",
        });
      };

      const error = new InvalidFIFOMessageError(
        "transactionId is required for FIFO messages"
      );

      expect(fun).toThrow(error);
    });

    it("should throw error if no groupId for fifo message", function () {
      const fun = () => {
        publisherFIFO.dispatch({
          type: Events.ORDER_PENDING,
          uri: "/api",
          checksum: 1,
          source: "test",
          message: "test",
          transactionId: "123",
        });
      };

      const error = new InvalidFIFOMessageError(
        "groupId is required for FIFO messages"
      );

      expect(fun).toThrow(error);
    });
  });
});