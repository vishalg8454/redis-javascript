const between = (ms, seq, startMs, startSeq, endMs, endSeq) => {
  return ms >= startMs && ms <= endMs && seq >= startSeq && seq <= endSeq;
};

const greater = (ms, seq, argMs, argSeq) => {
  if (ms < argMs) {
    return false;
  }
  if (ms === argMs) {
    return seq > argSeq;
  }
  return true;
};

module.exports = {
  between,
  greater,
};
