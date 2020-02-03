const assert = require('assert');

let addon = require('./dylib');

let obj = new addon.MyObject(10);
assert.equal(obj.value,10,"verify value works");
assert.equal(obj.plusOne(),11);

let obj2 = obj.multiply(-1);
assert.equal(obj2.value,-10);

obj.plusTwo(10).then( (val) => {
    console.log("plus two is ",val);
});

obj.multiply2(-1).then( (obj3) => {
    console.log("multiply two ",obj3.value);
});

obj.sleep((msg) => {
    assert.equal(msg,"hello world");;
});


addon.create(10).then( (test_object) => {
    console.log("test value is {}",test_object.value2());
});
