let addon = require('./dylib');

addon.hello(5).then((val) => {
  console.log("value is ",val);  // "value is 10"
});
