const {
  numberToRespInteger,
  arrayToRespString,
} = require("../conversionUtils");
const { store } = require("../store");

const pushHandler = (connection, isLeftPush, listKey, newListElements) => {
  const listExists = store.get(listKey);
  const existingValue = store.get(listKey)?.value;
  store.set(listKey, {
    value: listExists
      ? isLeftPush
        ? [...newListElements.reverse(), ...existingValue]
        : [...existingValue, ...newListElements]
      : [...newListElements],
    expiry: Infinity,
  });
  const updatedListLength = store.get(listKey).value.length;
  connection.write(numberToRespInteger(updatedListLength));
};

const lRangeHandler = (connection, listKey, startIndex, endIndex) => {
  const listExists = store.get(listKey);
  if (!listExists) {
    connection.write(arrayToRespString([]));
  }
  const list = store.get(listKey).value;
  const listLength = list.length;
  startIndex = startIndex < 0 ? listLength + startIndex : startIndex;
  endIndex = endIndex < 0 ? listLength + endIndex : endIndex;
  startIndex = startIndex < 0 ? 0 : startIndex;
  endIndex = endIndex < 0 ? 0 : endIndex;
  const result = list.slice(startIndex, endIndex + 1);
  connection.write(arrayToRespString(result));
};

const lLenHandler = (connection, listKey) => {
  const listExists = store.get(listKey);
  if (!listExists) {
    connection.write(numberToRespInteger(0));
  }
  const listLength = store.get(listKey).value.length;
  connection.write(numberToRespInteger(listLength));
};

const lPopHandler = (connection, listKey, countToRemove) => {
  const listExists = store.get(listKey);
  if (!listExists) {
    connection.write(nullBulkString);
  }
  const existingList = store.get(listKey).value;
  if (!existingList.length) {
    connection.write(nullBulkString);
  }
  const elementsToBeRemoved = existingList.slice(0, countToRemove);
  store.set(listKey, {
    value: existingList.slice(countToRemove),
    expiry: Infinity,
  });
  let responseString;
  if (countToRemove === 1) {
    //return string
    responseString = stringToBulkString(elementsToBeRemoved[0]);
  } else {
    //return array
    responseString = arrayToRespString(elementsToBeRemoved);
  }
  connection.write(responseString);
};

module.exports = {
  pushHandler,
  lRangeHandler,
  lLenHandler,
  lPopHandler,
};
