let addon = require('./dylib');
const assert = require('assert');

addon.hello(2,function(msg){
  console.log(msg); // 'argument is 2'
});
