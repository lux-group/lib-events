import { SNSMessage } from "aws-lambda";

declare module "@luxuryescapes/lib-events" {
  type Message = {
    type: string;
    source: string;
    id: string;
    uri: string;
    checksum: string;
  };

  type PollingOptions = {
    maxNumberOfMessages: number;
    maxIterations: number;
  }

  function poll(processMessage: Function, options: PollingOptions): Promise<void>;

  function mapAttributes(data: SNSMessage): Message;

  const ORDER_PENDING: string;
  const ORDER_COMPLETED: string;
  const ORDER_CANCELLED: string;
  const ORDER_REFUNDED: string;
  const ORDER_ABANDONED: string;
  const ORDER_AWAITING_PAYMENT: string;
  const ORDER_AWAITING_PURCHASE: string;
  const ORDER_NEEDS_ATTENTION: string;
  const ORDER_PAYMENT_FAILED: string;
  const ORDER_TOUCH: string;

  const ORDERS_CHECKSUM: string;
  const ORDERS_CHECKSUM_ERROR: string;

  const ORDER_ITEM_CREATED: string;
  const ORDER_ITEM_COMPLETED: string;
  const ORDER_ITEM_AWAITING_DATES: string;
  const ORDER_ITEM_FAILED: string;
  const ORDER_ITEM_CANCELLED: string;
  const ORDER_ITEM_TOUCH: string;
  const ORDER_ITEM_CHANGE_DATES: string;
  const ORDER_ITEM_DELETE_RESERVATION: string;
  const ORDER_ITEM_UPDATE_RESERVATION: string;
  const ORDER_ITEMS_CHECKSUM: string;
  const ORDER_ITEMS_CHECKSUM_ERROR: string;

  const ORDER_ADDON_ITEM_CANCELLED: string;

  const ORDER_FLIGHT_ITEM_CREATED: string;
  const ORDER_FLIGHT_ITEM_COMPLETED: string;
  const ORDER_FLIGHT_ITEM_FAILED: string;
  const ORDER_FLIGHT_ITEM_CANCELLED: string;

  const OFFER_UPDATE: string;

  const PROPERTY_UPDATE: string;
  const PROPERTY_DELETE: string;

  const ROOM_AVAILABILITY_UPDATE: string;
  const RATE_AVAILABILITY_UPDATE: string;

  const HOTEL_RESERVATION_SITEMINDER_ERROR: string;
  const HOTEL_RESERVATION_TRAVELCLICK_ERROR: string;

  const RESERVATION_FX_RATES_UPDATE: string;

  const SITEMINDER_CURRENCY_ERROR: string;

  const VOUCHER_UPDATE: string;

  const TOUR_UPDATE: string;
  const TOUR_DELETE: string;
}
