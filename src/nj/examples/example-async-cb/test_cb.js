let addon = require('./dylib');

addon.hello(function(val,msg){
  console.log(val,msg); // 10, 'hello world'
});
