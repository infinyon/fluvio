let addon = require('./dylib');
const EventEmitter = require('events').EventEmitter;
const emitter = new EventEmitter();

emitter.on('data', (evt) => {
    console.log("received event",evt);
})


addon.connect("sc:9003").then( sc => {
    console.log("connect to sc at ",sc.addr());
    
    sc.leader("test",0).then( leader => {
        
        /*
        leader.produce("new message").then( len => {
            console.log("message send");
        });
        */
    
        leader.consume(emitter.emit.bind(emitter));
       
    })
    
});

