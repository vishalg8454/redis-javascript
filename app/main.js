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
      if (arr[i].toLocaleUpperCase() === "RPUSH") {
        const listName = arr[i + 1];
        const newListElements = arr.slice(i + 2);
        const arrayExists = map.get(listName);
        const existingValue = map.get(listName).value;
        map.set(listName, {
          value: arrayExists
            ? [...existingValue, ...newListElements]
            : [newListElements],
          expiry: Infinity,
        });

        connection.write(`:${map.get(listName).value.length}\r\n`);
      }
    }
  });
});

server.listen(6379, "127.0.0.1");
