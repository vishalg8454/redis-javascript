const { stringToSimpleString } = require("../conversionUtils");

const pingHandler = (connection) => {
  connection.write(stringToSimpleString("PONG"));
};

const echoHandler = (connection, string) => {
  const echoString = stringToSimpleString(string);
  connection.write(echoString);
};

module.exports = {
  pingHandler,
  echoHandler,
};
