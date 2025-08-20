const net = require("net");
const { rParser } = require("./parser");

const EventEmitter = require("events");
const emitter = new EventEmitter();

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const map = new Map();

const waitList = new Map();

const checkWaitlist = (listKey) => {
  const queue = waitList.get(listKey);
  if (Array.isArray(queue) && queue.length > 0) {
    const front = queue.shift();
    waitList.set(listKey, queue);
    emitter.emit(front, listKey);
  }
};

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

const compare = (ms, seq, startMs, startSeq, endMs, endSeq) => {
  return ms >= startMs && ms <= endMs && seq >= startSeq && seq <= endSeq;
};

const server = net.createServer((connection) => {
  connection.on("data", (data) => {
    const str = data.toString();
    const arr = rParser(str);
    // console.log("parsed array", arr);
    for (let i = 0; i < arr.length; i++) {
      if (arr[i].toLocaleUpperCase() === "PING") {
        connection.write("+PONG\r\n");
      }
      if (arr[i].toLocaleUpperCase() === "ECHO") {
        const echoString = `+${arr[i + 1]}\r\n`;
        connection.write(echoString);
      }
      if (arr[i].toLocaleUpperCase() === "GET") {
        const key = arr[i + 1];
        const result = map.get(key);
        const value = result.value;
        const expiryTime = result.expiry;
        const expired = Date.now() > expiryTime;
        const resultString = expired
          ? `$-1\r\n`
          : `$${value.length}\r\n${value}\r\n`;
        console.log("map", map);
        connection.write(resultString);
      }
      if (arr[i].toLocaleUpperCase() === "SET") {
        const key = arr[i + 1];
        const value = arr[i + 2];
        const expiryPresent = arr[i + 3]?.toLocaleUpperCase() === "PX";
        const expiryTime = Number(arr[i + 4]);
        map.set(key, {
          value,
          expiry: expiryPresent ? Date.now() + expiryTime : Infinity,
        });

        connection.write("+OK\r\n");
      }
      if (["RPUSH", "LPUSH"].includes(arr[i].toLocaleUpperCase())) {
        const isLeftPush = arr[i] === "LPUSH";
        const listKey = arr[i + 1];
        const newListElements = arr.slice(i + 2);
        const arrayExists = map.get(listKey);
        const existingValue = map.get(listKey)?.value;
        map.set(listKey, {
          value: arrayExists
            ? isLeftPush
              ? [...newListElements.reverse(), ...existingValue]
              : [...existingValue, ...newListElements]
            : [...newListElements],
          expiry: Infinity,
        });
        connection.write(`:${map.get(listKey).value.length}\r\n`);
        checkWaitlist(listKey);
      }
      if (arr[i].toLocaleUpperCase() === "LRANGE") {
        const listKey = arr[i + 1];
        let startIndex = Number(arr[i + 2]);
        let endIndex = Number(arr[i + 3]);
        const arrayExists = map.get(listKey);
        if (!arrayExists) {
          connection.write(`*0\r\n`);
        }
        const arrLength = map.get(listKey).value.length;
        startIndex = startIndex < 0 ? arrLength + startIndex : startIndex;
        endIndex = endIndex < 0 ? arrLength + endIndex : endIndex;
        startIndex = startIndex < 0 ? 0 : startIndex;
        endIndex = endIndex < 0 ? 0 : endIndex;
        const arrayElements = map
          .get(listKey)
          .value.slice(startIndex, endIndex + 1);
        let responseString = `*${arrayElements.length}\r\n`;
        arrayElements.forEach((element) => {
          responseString += `$${element.length}\r\n${element}\r\n`;
        });
        connection.write(responseString);
      }
      if (arr[i].toLocaleUpperCase() === "LLEN") {
        const listKey = arr[i + 1];
        const arrayExists = map.get(listKey);
        if (!arrayExists) {
          connection.write(`:0\r\n`);
        }
        const arrLength = map.get(listKey).value.length;
        let responseString = `:${arrLength}\r\n`;
        connection.write(responseString);
      }
      if (arr[i].toLocaleUpperCase() === "LPOP") {
        const listKey = arr[i + 1];
        const countToRemove = Number(arr[i + 2]) || 1;
        const arrayExists = map.get(listKey);
        if (!arrayExists) {
          connection.write(`$-1\r\n`);
        }
        const existingArray = map.get(listKey).value;
        if (!existingArray.length) {
          connection.write(`$-1\r\n`);
        }
        const elementsToBeRemoved = existingArray.slice(0, countToRemove);
        map.set(listKey, {
          value: existingArray.slice(countToRemove),
          expiry: Infinity,
        });
        let responseString = "";
        if (countToRemove === 1) {
          //return string
          responseString += `$${elementsToBeRemoved[0].length}\r\n${elementsToBeRemoved[0]}\r\n`;
        } else {
          //return array
          responseString += `*${elementsToBeRemoved.length}\r\n`;
          elementsToBeRemoved.forEach((element) => {
            responseString += `$${element.length}\r\n${element}\r\n`;
          });
        }
        connection.write(responseString);
      }
      if (arr[i].toLocaleUpperCase() === "BLPOP") {
        const clientAddress = `${connection.remoteAddress}:${connection.remotePort}`;
        const listKey = arr[i + 1];
        const timeout = Number(arr[i + 2]);
        let timeoutId;

        if (timeout !== 0) {
          timeoutId = setTimeout(() => {
            connection.write("$-1\r\n");
            let queue = waitList.get(listKey);
            if (Array.isArray(queue) && queue.length > 0) {
              queue = queue.filter((it) => it !== clientAddress);
              waitList.set(listKey, queue);
            }
          }, timeout * 1000);
        }

        const existingArray = map.get(listKey)?.value || [];
        if (existingArray.length > 0) {
          //we have an element ready
          const elementToBeRemoved = existingArray[0];
          map.set(listKey, {
            value: existingArray.slice(1),
            expiry: Infinity,
          });
          connection.write(
            `$${elementToBeRemoved.length}\r\n${elementToBeRemoved}\r\n`
          );
        }
        const previousQueue = waitList.get(listKey) || [];
        waitList.set(listKey, [...previousQueue, clientAddress]);
        emitter.once(clientAddress, (listKey) => {
          const existingArray = map.get(listKey).value;
          const elementToBeRemoved = existingArray[0];
          map.set(listKey, {
            value: existingArray.slice(1),
            expiry: Infinity,
          });
          let responseString = "";
          responseString += `*2\r\n`;
          responseString += `$${listKey.length}\r\n${listKey}\r\n`;
          responseString += `$${elementToBeRemoved.length}\r\n${elementToBeRemoved}\r\n`;
          connection.write(responseString);
          clearTimeout(timeoutId);
        });
      }
      if (arr[i].toLocaleUpperCase() === "TYPE") {
        const itemKey = arr[i + 1];
        const result = map.get(itemKey);
        const value = result?.value;
        if (Array.isArray(result)) {
          connection.write("+stream\r\n");
        }
        if (!value) {
          connection.write("+none\r\n");
        }
        if (Array.isArray(value)) {
          connection.write("+list\r\n");
        }
        if (typeof value === "string") {
          connection.write("+string\r\n");
        }
      }
      if (arr[i].toLocaleUpperCase() === "XADD") {
        const itemKey = arr[i + 1];
        const id = arr[i + 2];
        const result = map.get(itemKey);
        let actualId = "";
        //id can be * | <ms>-* | <ms>-<seq>
        if (id === "*") {
          actualId = `${Date.now()}-${0}`;
          //not impl this case due laziness(If the time already exists in the stream, the sequence number for that record incremented by one will be used.)
        } else if (id.split("-")[1] === "*") {
          const receivedMs = Number(id.split("-")[0]);
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
          let valid = true;
          const receivedMs = Number(id.split("-")[0]);
          const receivedSeq = Number(id.split("-")[1]);
          if (receivedMs < 0 || receivedSeq < 0) {
            valid = false;
          }
          if (receivedMs === 0) {
            valid = receivedSeq > 0;
          }
          if (receivedMs > 0) {
            valid = receivedMs >= 0;
          }
          if (!valid) {
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
              valid = receivedSeq > seq;
            } else {
              valid = receivedMs >= ms;
            }
          }
          if (!valid) {
            connection.write(
              "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
            );
            break;
          }
          actualId = id;
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
        map.set(itemKey, arrayOfNewItems);
        connection.write(`$${actualId.length}\r\n${actualId}\r\n`);
      }
      if (arr[i].toLocaleUpperCase() === "XRANGE") {
        const [itemKey, startId, endId] = arr.slice(i + 1, i + 4);
        const startMs = startId === "-" ? 0 : Number(startId.split("-")[0]);
        const startSeq =
          startId === "-" ? 0 : Number(startId.split("-")[1]) ?? 0;
        const endMs = endId === "+" ? Infinity : Number(endId.split("-")[0]);
        const endSeq =
          endId === "+" ? Infinity : Number(endId.split("-")[1]) ?? 0;
        const result = map.get(itemKey);
        let responseArr = [];
        if (result) {
          for (let i = 0; i < result.length; i++) {
            const it = result[i];
            const { ms, seq, kv } = it;
            if (ms > endMs) {
              break;
            }
            const localArr = [];
            if (compare(ms, seq, startMs, startSeq, endMs, endSeq)) {
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
      if (arr[i].toLocaleUpperCase() === "XREAD") {
        const keyAndIdArgs = arr.splice(2);
        const arrOfKeyAndIds = [];
        for (let i = 0; i < keyAndIdArgs.length / 2; i++) {
          const key = keyAndIdArgs[i];
          const id = keyAndIdArgs[i + keyAndIdArgs.length / 2];
          arrOfKeyAndIds.push([key, id]);
        }
        const responseArr = [];
        arrOfKeyAndIds.forEach((it) => {
          const currentKey = it[0];
          const currentId = it[1];
          const currentMs = Number(currentId.split("-")[0]);
          const arrForCurrentKey = [];
          arrForCurrentKey.push(currentKey);
          const resultForCurrentKey = [];
          const result = map.get(currentKey);
          if (result) {
            for (let i = 0; i < result.length; i++) {
              const it = result[i];
              const { ms, seq, kv } = it;
              if (ms > currentMs) {
                const localArr = [];
                localArr.push(String(ms) + "-" + String(seq));
                kv.forEach((it) => {
                  localArr.push(it.key);
                  localArr.push(it.value);
                });
                resultForCurrentKey.push(localArr);
              }
            }
            arrForCurrentKey.push(resultForCurrentKey);
            responseArr.push(arrForCurrentKey);
          }
        });
        connection.write(arrayToRespString(responseArr));
      }
    }
  });
});

server.listen(6379, "127.0.0.1");
