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
    }
  });
});

server.listen(6379, "127.0.0.1");
