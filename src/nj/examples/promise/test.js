const assert = require('assert');
let addon = require('./dylib');

addon.hello(5).then((val) => {
  assert.equal(val,15);
  console.log("promise test succeed: {}",val);
});
