const { stringToSimpleString } = require("../conversionUtils");

const pingHandler = (connection) => {
  connection.write(stringToSimpleString("PONG"));
};

const echoHandler = (connection, string) => {
  const echoString = stringToSimpleString(arr[i + 1]);
  connection.write(echoString);
};

module.exports = {
  pingHandler,
  echoHandler,
};
