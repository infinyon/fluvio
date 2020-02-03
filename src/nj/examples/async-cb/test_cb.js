let addon = require('./dylib');
const assert = require('assert');

addon.hello(function(val,msg){
  assert.equal(val,10);
  assert.equal(msg,"hello world");
  console.log("callback test succeed");
});

