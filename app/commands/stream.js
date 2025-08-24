const { stringToBulkString, arrayToRespString } = require("../conversionUtils");
const EventEmitter = require("events");
const { store } = require("../store");
const { greater, between } = require("../utils");

const streamEmitter = new EventEmitter();
const streamWaitList = new Map();

const checkStreamWaitList = (streamKey) => {
  const queue = streamWaitList.get(streamKey);
  if (Array.isArray(queue) && queue.length > 0) {
    const front = queue.shift();
    streamWaitList.set(streamKey, queue);
    streamEmitter.emit(front, streamKey);
  }
};

const xAddHandler = (connection, itemKey, receivedId, kVPairs) => {
  const result = store.get(itemKey);
  let actualId = "";
  //id can be * | <ms>-* | <ms>-<seq>
  if (receivedId === "*") {
    actualId = `${Date.now()}-${0}`;
    //not impl this case due laziness(If the time already exists in the stream, the sequence number for that record incremented by one will be used.)
  } else if (receivedId.split("-")[1] === "*") {
    const receivedMs = Number(receivedId.split("-")[0]);
    if (result) {
      const lastElement = result.at(-1);
      const lastElementMs = lastElement.ms;
      const lastElementSeq = lastElement.seq;
      if (receivedMs === lastElementMs) {
        actualId = `${lastElementMs}-${lastElementSeq + 1}`;
      } else {
        actualId = `${receivedMs}-${receivedMs === 0 ? 1 : 0}`;
      }
    } else {
      actualId = `${receivedMs}-${receivedMs === 0 ? 1 : 0}`;
    }
  } else {
    let isReceivedIdValid = true;
    const receivedMs = Number(receivedId.split("-")[0]);
    const receivedSeq = Number(receivedId.split("-")[1]);
    if (receivedMs < 0 || receivedSeq < 0) {
      isReceivedIdValid = false;
    }
    if (receivedMs === 0) {
      isReceivedIdValid = receivedSeq > 0;
    }
    if (receivedMs > 0) {
      isReceivedIdValid = receivedMs >= 0;
    }
    if (!isReceivedIdValid) {
      connection.write(
        "-ERR The ID specified in XADD must be greater than 0-0\r\n"
      );
      return;
    }
    if (result) {
      const lastElement = result.at(-1);
      const ms = lastElement.ms;
      const seq = lastElement.seq;
      if (receivedMs === ms) {
        isReceivedIdValid = receivedSeq > seq;
      } else {
        isReceivedIdValid = receivedMs >= ms;
      }
    }
    if (!isReceivedIdValid) {
      connection.write(
        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
      );
      return;
    }
    actualId = receivedId;
  }
  let arrayOfNewItems = result ? [...result] : [];
  const arrForReceivedItems = [];
  for (let i = 0; i < kVPairs.length; i += 2) {
    const key = kVPairs[i];
    const value = kVPairs[i + 1];
    arrForReceivedItems.push({ key, value });
  }
  const ms = Number(actualId.split("-")[0]);
  const seq = Number(actualId.split("-")[1]);
  arrayOfNewItems.push({ ms, seq, kv: arrForReceivedItems });
  store.set(itemKey, arrayOfNewItems);
  connection.write(stringToBulkString(actualId));
  checkStreamWaitList(itemKey);
};

const xRangeHandler = (connection, itemKey, startId, endId) => {
  const startMs = startId === "-" ? 0 : Number(startId.split("-")[0]);
  const startSeq = startId === "-" ? 0 : Number(startId.split("-")[1]) ?? 0;
  const endMs = endId === "+" ? Infinity : Number(endId.split("-")[0]);
  const endSeq = endId === "+" ? Infinity : Number(endId.split("-")[1]) ?? 0;
  const result = store.get(itemKey);
  let responseArr = [];
  if (result) {
    for (let i = 0; i < result.length; i++) {
      const it = result[i];
      const { ms, seq, kv } = it;
      if (ms > endMs) {
        break;
      }
      const localArr = [];
      if (between(ms, seq, startMs, startSeq, endMs, endSeq)) {
        localArr.push(String(ms) + "-" + String(seq));
        const kvArray = [];
        kv.forEach((it) => {
          kvArray.push(it.key);
          kvArray.push(it.value);
        });
        localArr.push(kvArray);
        responseArr.push(localArr);
      }
    }
  }
  connection.write(arrayToRespString(responseArr));
};

const xReadHandler = (
  connection,
  isBlockingMode,
  blockingTime,
  keyAndIdArgs
) => {
  const arrOfKeyAndIds = [];
  for (let i = 0; i < keyAndIdArgs.length / 2; i++) {
    const key = keyAndIdArgs[i];
    const id = keyAndIdArgs[i + keyAndIdArgs.length / 2];
    arrOfKeyAndIds.push([key, id]);
  }
  const responseArr = [];
  let someDataReturned = false;
  arrOfKeyAndIds.forEach((it) => {
    const currentKey = it[0];
    const currentId = it[1];
    const temp = store.get(currentKey);
    const lastMs = temp.length ? temp.at(-1).ms : -1;
    const lastSeq = temp.length ? temp.at(-1).seq : -1;
    const currentMs =
      currentId === "$" ? lastMs : Number(currentId.split("-")[0]);
    const currentSeq =
      currentId === "$" ? lastSeq : Number(currentId.split("-")[1]);
    const arrForCurrentKey = [];
    arrForCurrentKey.push(currentKey);
    const resultForCurrentKey = [];
    const result = store.get(currentKey);
    if (result) {
      for (let i = 0; i < result.length; i++) {
        const it = result[i];
        const { ms, seq, kv } = it;
        if (greater(ms, seq, currentMs, currentSeq)) {
          someDataReturned = true;
          const localArr = [];
          localArr.push(String(ms) + "-" + String(seq));
          const kvArray = [];
          kv.forEach((it) => {
            kvArray.push(it.key);
            kvArray.push(it.value);
          });
          localArr.push(kvArray);
          resultForCurrentKey.push(localArr);
        }
      }
      arrForCurrentKey.push(resultForCurrentKey);
      responseArr.push(arrForCurrentKey);
    }
  });
  if (someDataReturned) {
    connection.write(arrayToRespString(responseArr));
  }
  if (!someDataReturned && isBlockingMode) {
    const clientAddress = `${connection.remoteAddress}:${connection.remotePort}`;
    arrOfKeyAndIds.forEach((it) => {
      const currentKey = it[0];
      const currentId = it[1];
      const temp = store.get(currentKey);
      const lastMs = temp.length ? temp.at(-1).ms : -1;
      const lastSeq = temp.length ? temp.at(-1).seq : -1;
      const currentMs =
        currentId === "$" ? lastMs : Number(currentId.split("-")[0]);
      const currentSeq =
        currentId === "$" ? lastSeq : Number(currentId.split("-")[1]);

      let timeoutId;

      if (blockingTime) {
        timeoutId = setTimeout(() => {
          connection.write(nullBulkString);
          let queue = streamWaitList.get(currentKey);
          if (Array.isArray(queue) && queue.length > 0) {
            queue = queue.filter((it) => it !== clientAddress);
            streamWaitList.set(currentKey, queue);
          }
        }, blockingTime);
      }

      const previousQueue = streamWaitList.get(currentKey) || [];
      streamWaitList.set(currentKey, [...previousQueue, clientAddress]);
      streamEmitter.once(clientAddress, (streamKey) => {
        const responseArr = [];
        const result = store.get(currentKey);
        const arrForCurrentKey = [];
        arrForCurrentKey.push(currentKey);
        const resultForCurrentKey = [];
        if (result) {
          for (let i = 0; i < result.length; i++) {
            const it = result[i];
            const { ms, seq, kv } = it;
            if (greater(ms, seq, currentMs, currentSeq)) {
              someDataReturned = true;
              const localArr = [];
              localArr.push(String(ms) + "-" + String(seq));
              const kvArray = [];
              kv.forEach((it) => {
                kvArray.push(it.key);
                kvArray.push(it.value);
              });
              localArr.push(kvArray);
              resultForCurrentKey.push(localArr);
            }
          }
          arrForCurrentKey.push(resultForCurrentKey);
          responseArr.push(arrForCurrentKey);
        }
        connection.write(arrayToRespString(responseArr));
        clearTimeout(timeoutId);
      });
    });
  }
};

module.exports = {
  xAddHandler,
  xRangeHandler,
  xReadHandler,
};
