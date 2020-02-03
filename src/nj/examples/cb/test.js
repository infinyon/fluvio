let addon = require('./dylib');
const assert = require('assert');

addon.hello(2,function(msg){
  assert.equal(msg,"argument is: 2");
  console.log("callback test succeed");
});
