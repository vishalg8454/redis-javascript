const { store } = require("../store");

const getHandler = (connection, key) => {
  const result = store.get(key);
  const value = result.value;
  const expiryTime = result.expiry;
  const expired = Date.now() > expiryTime;
  const resultString = expired ? nullBulkString : stringToBulkString(value);
  connection.write(resultString);
};

const setHandler = (connection, key, value, expiryPresent, expiryTime) => {
  store.set(key, {
    value,
    expiry: expiryPresent ? Date.now() + expiryTime : Infinity,
  });
  connection.write(stringToSimpleString("OK"));
};

module.exports = {
  getHandler,
  setHandler,
};
