let addon = require('./lib');

console.log("start");
addon(function(msg){
    console.log(msg); // 'hello world'
  });

  console.log("finish");
