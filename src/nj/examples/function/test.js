let addon = require('./dylib');
const assert = require('assert');

assert.equal(addon.hello(2),"hello world 2"); // 'hello world'
assert.throws( () => addon.hello("hello"));       // wrong argument type
assert.throws(() => addon.hello());       // wrong argument count
console.log("function tests succeed");
