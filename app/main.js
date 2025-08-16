const net = require("net");

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const isNumChar = (char) => char >= 0 && char <= 9;
const isChar = (char) =>
  (char >= "a" && char <= "z") || (char >= "A" && char <= "Z");

const rParser = (str) => {
  let n = str.length;
  let arr = [];
  for (let i = 0; i < n; i++) {
    if (str[i] === "*") {
      //array
      let numElements = "";
      while (isNumChar(str[i + 1])) {
        numElements += str[i + 1];
        i++;
      }
    }
    if (str[i] === "$") {
      //string
      let stringLength = "";
      while (isNumChar(str[i + 1])) {
        stringLength += str[i + 1];
        i++;
      }
      let localString = "";
      let localStringLen = Number(stringLength);
      for (let j = 0; j < localStringLen; j++, i++) {
        localString += str[i + 1];
      }
      arr.push(localString);
    }
  }
  return arr;
};

// Uncomment this block to pass the first stage
const server = net.createServer((connection) => {
  connection.on("data", (data) => {
    const str = data.toString();
    const arr = rParser(str);
    console.log("parsed array", arr);
    for (let i = 0; i < arr.length; i++) {
      if (arr[i].toLocaleUpperCase === "PING") {
        connection.write("+PONG\r\n");
      }
      if (arr[i].toLocaleUpperCase === "ECHO") {
        const echoString = `+${arr[i + 1]}\r\n`;
        console.log("echoing", echoString);
        connection.write(echoString);
      }
    }
  });
});

server.listen(6379, "127.0.0.1");
