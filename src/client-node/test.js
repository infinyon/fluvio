// run test
// this assume stream controller is running at localhost: 9003
// use flv-integration-test to run locally

let addon = require('./dist');
// let addon = require('@fluvio/client');


const EventEmitter = require('events').EventEmitter;
const emitter = new EventEmitter();

console.log("loaded client");

emitter.on('data', (evt) => {
    console.log("received event",evt);
})

console.log("connecting client to sc");
addon.connect("localhost:9003").then( sc => {
    console.log("connect to sc at ",sc.addr());
    
    sc.replica("test1",0).then( leader => {
        
        try {

            leader.consume( {
                        offset: "earliest",
                        includeMetadata: true,
                        type: 'text',
                        isolation: 'readCommitted'
                    },
                emitter.emit.bind(emitter)
            );
        } catch(ex) {
            console.log(ex);
        } 
    })
})
.catch((err) => console.log(err));

