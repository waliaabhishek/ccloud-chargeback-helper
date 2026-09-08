import { describe, expect, it } from "vitest";
import { formatDecimalCurrency } from "./decimalCurrency";

describe("formatDecimalCurrency", () => {
  it.each([
    ["0.3", "$0.30"],
    ["0", "$0.00"],
    ["-12.5", "-$12.50"],
    ["12345678901234567890.123456789", "$12,345,678,901,234,567,890.12"],
    ["1.999", "$2.00"],
    ["-0.005", "-$0.01"],
  ])("formats %s exactly as %s without floating point conversion", (value, expected) => {
    expect(formatDecimalCurrency(value)).toBe(expected);
  });
});
