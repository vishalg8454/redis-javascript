const net = require("net");
const { rParser } = require("./parser");

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

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
    }
  });
});

server.listen(6379, "127.0.0.1");
