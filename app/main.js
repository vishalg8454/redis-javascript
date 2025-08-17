const net = require("net");
const { rParser } = require("./parser");

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const map = new Map();

const server = net.createServer((connection) => {
  connection.on("data", (data) => {
    const str = data.toString();
    const arr = rParser(str);
    console.log("parsed array", arr);
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
          connection.write(`*0\r\n`);
        }
        const arrLength = map.get(listKey).value.length;
        let responseString = `$${arrLength}\r\n`;
        connection.write(responseString);
      }
    }
  });
});

server.listen(6379, "127.0.0.1");
