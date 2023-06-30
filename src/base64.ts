// SNS -> SQS subscription could not filter message
// if the message contains a stringified JSON
// use base64 to encode str instead
// https://stackoverflow.com/questions/59853890/sns-subscription-filter-policies-do-not-seem-to-work-when-a-binary-message-attri

export function encodeJson<T = unknown>(json: T): string {
  const jsonStr = JSON.stringify(json);
  const buff = Buffer.from(jsonStr);
  return buff.toString("base64");
}

export function decodeJson<T = unknown>(base64: string): T {
  const buff = Buffer.from(base64, "base64");
  const jsonStr = buff.toString("utf-8");
  return JSON.parse(jsonStr) as T;
}
