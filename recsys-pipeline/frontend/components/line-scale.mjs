// LineChart's vertical scale, kept pure so it is tested by running it.
//
// Maps [lo, hi] of the observed values onto the plot, top = hi. A flat series (hi == lo) has no
// range to map: it is drawn through the middle and labelled once, rather than sitting on the
// floor with the same value printed at both ends of the axis.
export function lineScale(observed, height, pad) {
  const lo = Math.min(...observed);
  const hi = Math.max(...observed);
  const flat = hi === lo;
  const y = (v) => (flat ? height / 2 : height - pad - ((v - lo) / (hi - lo)) * (height - 2 * pad));
  return { lo, hi, flat, y };
}
