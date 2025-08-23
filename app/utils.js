const stringToBulkString = (str) => {
  return `$${str.length}\r\n${str}\r\n`;
};

const arrayToRespString = (arr) => {
  let str = "";
  str += `*${arr.length}\r\n`;
  arr.forEach((it) => {
    if (typeof it === "string") {
      str += stringToBulkString(it);
    } else if (Array.isArray(it)) {
      str += arrayToRespString(it);
    }
  });
  return str;
};

module.exports = { stringToBulkString, arrayToRespString };
