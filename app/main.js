const net = require("net");
const { rParser } = require("./parser");
const {
  arrayToRespString,
  stringToBulkString,
  numberToRespInteger,
  nullBulkString,
  stringToSimpleString,
} = require("./conversionUtils");

const EventEmitter = require("events");
const { greater, between } = require("./utils");
const listEmitter = new EventEmitter();
const streamEmitter = new EventEmitter();

const store = new Map();

const listWaitList = new Map();
const streamWaitList = new Map();

const checkListWaitList = (listKey) => {
  const queue = listWaitList.get(listKey);
  if (Array.isArray(queue) && queue.length > 0) {
    const front = queue.shift();
    listWaitList.set(listKey, queue);
    listEmitter.emit(front, listKey);
  }
};

const checkStreamWaitList = (streamKey) => {
  const queue = streamWaitList.get(streamKey);
  if (Array.isArray(queue) && queue.length > 0) {
    const front = queue.shift();
    streamWaitList.set(streamKey, queue);
    streamEmitter.emit(front, streamKey);
  }
};

const server = net.createServer((connection) => {
  connection.on("data", (data) => {
    const str = data.toString();
    const arr = rParser(str);
    for (let i = 0; i < arr.length; i++) {
      const commandName = arr[i].toLocaleUpperCase();
      if (commandName === "PING") {
        connection.write(stringToSimpleString("PONG"));
      }
      if (commandName === "ECHO") {
        const echoString = stringToSimpleString(arr[i + 1]);
        connection.write(echoString);
      }
      if (commandName === "GET") {
        const key = arr[i + 1];

        const result = store.get(key);
        const value = result.value;
        const expiryTime = result.expiry;
        const expired = Date.now() > expiryTime;
        const resultString = expired
          ? nullBulkString
          : stringToBulkString(value);
        connection.write(resultString);
      }
      if (commandName === "SET") {
        const key = arr[i + 1];
        const value = arr[i + 2];
        const expiryPresent = arr[i + 3]?.toLocaleUpperCase() === "PX";
        const expiryTime = Number(arr[i + 4]);

        store.set(key, {
          value,
          expiry: expiryPresent ? Date.now() + expiryTime : Infinity,
        });

        connection.write(stringToSimpleString("OK"));
      }
      if (["RPUSH", "LPUSH"].includes(commandName)) {
        const isLeftPush = commandName === "LPUSH";
        const listKey = arr[i + 1];
        const newListElements = arr.slice(i + 2);

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
      }
      if (commandName === "LRANGE") {
        const listKey = arr[i + 1];
        let startIndex = Number(arr[i + 2]);
        let endIndex = Number(arr[i + 3]);

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
      }
      if (commandName === "LLEN") {
        const listKey = arr[i + 1];

        const listExists = store.get(listKey);
        if (!listExists) {
          connection.write(numberToRespInteger(0));
        }
        const listLength = store.get(listKey).value.length;
        connection.write(numberToRespInteger(listLength));
      }
      if (commandName === "LPOP") {
        const listKey = arr[i + 1];
        const countToRemove = Number(arr[i + 2]) || 1;

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
      }
      if (commandName === "BLPOP") {
        const clientAddress = `${connection.remoteAddress}:${connection.remotePort}`;
        const listKey = arr[i + 1];
        const timeout = Number(arr[i + 2]);

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
      }
      if (commandName === "TYPE") {
        const itemKey = arr[i + 1];

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
      }
      if (commandName === "XADD") {
        const itemKey = arr[i + 1];
        const receivedId = arr[i + 2];

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
            break;
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
            break;
          }
          actualId = receivedId;
        }
        const kVPairs = arr.splice(3);
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
      }
      if (commandName === "XRANGE") {
        const [itemKey, startId, endId] = arr.slice(i + 1, i + 4);

        const startMs = startId === "-" ? 0 : Number(startId.split("-")[0]);
        const startSeq =
          startId === "-" ? 0 : Number(startId.split("-")[1]) ?? 0;
        const endMs = endId === "+" ? Infinity : Number(endId.split("-")[0]);
        const endSeq =
          endId === "+" ? Infinity : Number(endId.split("-")[1]) ?? 0;
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
      }
      if (commandName === "XREAD") {
        const isBlockingMode = arr[i + 1].toLocaleUpperCase() === "BLOCK";
        const blockingTime = isBlockingMode ? Number(arr[i + 2]) : null;

        const keyAndIdArgs = arr.splice(isBlockingMode ? 4 : 2);
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
      }
    }
  });
});

server.listen(6379, "127.0.0.1");
