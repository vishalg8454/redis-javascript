const {
  numberToRespInteger,
  arrayToRespString,
  stringToBulkString,
} = require("../conversionUtils");
const EventEmitter = require("events");
const { store } = require("../store");

const listEmitter = new EventEmitter();
const listWaitList = new Map();

const checkListWaitList = (listKey) => {
  const queue = listWaitList.get(listKey);
  if (Array.isArray(queue) && queue.length > 0) {
    const front = queue.shift();
    listWaitList.set(listKey, queue);
    listEmitter.emit(front, listKey);
  }
};

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
  checkListWaitList(listKey);
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

const blPopHandler = (connection, listKey, timeout) => {
  const clientAddress = `${connection.remoteAddress}:${connection.remotePort}`;
  let timeoutId;
  if (timeout !== 0) {
    //if timeout is not infinite then we now need to return null, because if list contained element by now then emitter would have fired
    //and this timeout cancelled.
    timeoutId = setTimeout(() => {
      connection.write(nullBulkString);
      let queue = listWaitList.get(listKey);
      if (Array.isArray(queue) && queue.length > 0) {
        queue = queue.filter((it) => it !== clientAddress);
        listWaitList.set(listKey, queue);
      }
    }, timeout * 1000);
  }

  const list = store.get(listKey)?.value || [];
  if (list.length > 0) {
    //we have an element ready
    const elementToBeRemoved = list[0];
    store.set(listKey, {
      value: list.slice(1),
      expiry: Infinity,
    });
    const responseArray = [listKey, elementToBeRemoved];
    connection.write(arrayToRespString(responseArray));
  }
  const previousQueue = listWaitList.get(listKey) || [];
  listWaitList.set(listKey, [...previousQueue, clientAddress]);
  listEmitter.once(clientAddress, (listKey) => {
    const list = store.get(listKey).value;
    const elementToBeRemoved = list[0];
    store.set(listKey, {
      value: list.slice(1),
      expiry: Infinity,
    });
    const responseArray = [listKey, elementToBeRemoved];
    connection.write(arrayToRespString(responseArray));
    clearTimeout(timeoutId);
  });
};

module.exports = {
  pushHandler,
  lRangeHandler,
  lLenHandler,
  lPopHandler,
  blPopHandler,
};
