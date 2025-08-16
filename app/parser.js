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
      console.log(2, localStringLen);
      for (let j = 0; j < localStringLen; j++, i++) {
        localString += str[i + 1];
      }
      arr.push(localString);
    }
    if (str[i] === ":") {
      //number
      let numberString = "";
      while (isNumChar(str[i + 1])) {
        numberString += str[i + 1];
        i++;
      }
      const number = Number(numberString);
      console.log(3, numberString);
      arr.push(number);
    }
  }
  return arr;
};

module.exports = { rParser };
