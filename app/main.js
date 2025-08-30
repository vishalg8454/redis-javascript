const net = require("net");
const { rParser } = require("./parser");

const { pingHandler, echoHandler, typeHandler } = require("./commands/misc");
const { getHandler, setHandler } = require("./commands/string");
const {
  lRangeHandler,
  lPopHandler,
  pushHandler,
  lLenHandler,
  blPopHandler,
} = require("./commands/list");
const {
  xAddHandler,
  xRangeHandler,
  xReadHandler,
} = require("./commands/stream");
const { arrayToRespString, numberToRespInteger } = require("./conversionUtils");
const { store } = require("./store");

const server = net.createServer((connection) => {
  connection.on("data", (data) => {
    const str = data.toString();
    const arr = rParser(str);
    for (let i = 0; i < arr.length; i++) {
      const commandName = arr[i].toLocaleUpperCase();
      if (commandName === "PING") {
        pingHandler(connection);
      }
      if (commandName === "ECHO") {
        const echoString = arr[i + 1];

        echoHandler(connection, echoString);
      }
      if (commandName === "GET") {
        const key = arr[i + 1];

        getHandler(connection, key);
      }
      if (commandName === "SET") {
        const key = arr[i + 1];
        const value = arr[i + 2];
        const expiryPresent = arr[i + 3]?.toLocaleUpperCase() === "PX";
        const expiryTime = Number(arr[i + 4]);

        setHandler(connection, key, value, expiryPresent, expiryTime);
      }
      if (["RPUSH", "LPUSH"].includes(commandName)) {
        const isLeftPush = commandName === "LPUSH";
        const listKey = arr[i + 1];
        const newListElements = arr.slice(i + 2);

        pushHandler(connection, isLeftPush, listKey, newListElements);
      }
      if (commandName === "LRANGE") {
        const listKey = arr[i + 1];
        let startIndex = Number(arr[i + 2]);
        let endIndex = Number(arr[i + 3]);

        lRangeHandler(connection, listKey, startIndex, endIndex);
      }
      if (commandName === "LLEN") {
        const listKey = arr[i + 1];

        lLenHandler(connection, listKey);
      }
      if (commandName === "LPOP") {
        const listKey = arr[i + 1];
        const countToRemove = Number(arr[i + 2]) || 1;

        lPopHandler(connection, listKey, countToRemove);
      }
      if (commandName === "BLPOP") {
        const listKey = arr[i + 1];
        const timeout = Number(arr[i + 2]);

        blPopHandler(connection, listKey, timeout);
      }
      if (commandName === "TYPE") {
        const itemKey = arr[i + 1];

        typeHandler(connection, itemKey);
      }
      if (commandName === "XADD") {
        const itemKey = arr[i + 1];
        const receivedId = arr[i + 2];
        const kVPairs = arr.splice(3);

        xAddHandler(connection, itemKey, receivedId, kVPairs);
      }
      if (commandName === "XRANGE") {
        const [itemKey, startId, endId] = arr.slice(i + 1, i + 4);

        xRangeHandler(connection, itemKey, startId, endId);
      }
      if (commandName === "XREAD") {
        const isBlockingMode = arr[i + 1].toLocaleUpperCase() === "BLOCK";
        const blockingTime = isBlockingMode ? Number(arr[i + 2]) : null;

        const keyAndIdArgs = arr.splice(isBlockingMode ? 4 : 2);
        xReadHandler(connection, isBlockingMode, blockingTime, keyAndIdArgs);
      }
      if (commandName === "CONFIG") {
        connection.write(arrayToRespString([[]]));
        //implement bare minimum config so that redis-benchmark does not crash
        break;
      }
      if (commandName === "INCR") {
        const key = arr[i + 1];
        const result = store.get(key);
        const value = result.value;
        const newValue = Number(value + 1);
        if (typeof value === "string") {
          store.set(key, {
            ...result,
            value: String(newValue),
          });
          connection.write(numberToRespInteger(newValue));
        }
      }
    }
  });
});

server.listen(6379, "127.0.0.1");
