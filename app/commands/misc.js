const { stringToSimpleString } = require("../conversionUtils");

const pingHandler = (connection) => {
  connection.write(stringToSimpleString("PONG"));
};

const echoHandler = (connection, string) => {
  const echoString = stringToSimpleString(string);
  connection.write(echoString);
};

const typeHandler = (connection, itemKey) => {
  const result = store.get(itemKey);
  const value = result?.value;
  if (Array.isArray(result)) {
    connection.write(stringToSimpleString("stream"));
  }
  if (!value) {
    connection.write(stringToSimpleString("none"));
  }
  if (Array.isArray(value)) {
    connection.write(stringToSimpleString("list"));
  }
  if (typeof value === "string") {
    connection.write(stringToSimpleString("string"));
  }
};

module.exports = {
  pingHandler,
  echoHandler,
  typeHandler,
};
