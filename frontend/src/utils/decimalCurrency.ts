/** Format an API decimal string without converting it through a JS number. */
export function formatDecimalCurrency(
  value: string,
  currencySymbol = "$",
): string {
  const normalized = value.trim();
  const match = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(normalized);
  if (!match) {
    throw new Error(`Invalid decimal value: ${value}`);
  }

  const negative = match[1] === "-";
  const integerPart = BigInt(match[2]);
  const fractionPart = match[3] ?? "";
  const centsText = `${fractionPart}00`.slice(0, 2);
  let cents = BigInt(centsText);

  if ((fractionPart[2] ?? "0") >= "5") {
    cents += 1n;
  }

  let whole = integerPart;
  if (cents >= 100n) {
    whole += 1n;
    cents -= 100n;
  }

  const wholeText = whole.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
  const amount = `${wholeText}.${cents.toString().padStart(2, "0")}`;
  return `${negative ? "-" : ""}${currencySymbol}${amount}`;
}
