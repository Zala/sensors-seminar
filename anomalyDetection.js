const qm = require("qminer");
var assert = require('assert');
var distributionModel = require('./distributionModel.js');

var _winsize = 2 * 7 * 24 * 3600 * 1000;

var quantileEps = 0.01;
var countEps = 0.0001;
var maxRelErr = quantileEps + 2*countEps;
console.log('Quantile granularity: 0.01')
var targetQuants = (function () {
    var quants = [];
    for (var prob = 0; prob <= 1; prob += 0.01) {
        quants.push(prob);
    }
    return quants;
})();

// ---------------------------
// SCHEMA
//
let base = new qm.Base({
    mode: 'createClean',
    schema:[
        {
            name: 'Users',
            fields: [
                { name: 'Time', type: 'datetime' },
                { name: 'User', type: 'string' },
                { name: 'TimeBetween', type: 'int' }
            ]
        }
    ]
});

// SET1
// reads from record, exposes a time series
let tick1 = new qm.StreamAggr (base, {
    type: "timeSeriesTick",
    store: "Users",
    timestamp: "Time",
    value: "TimeBetween"
});

// reads from a time series, exposes a buffer
console.log('Window buffer: 1 week');
let winbuf1 = new qm.StreamAggr(base, {
    type: "timeSeriesWinBufVector",
    inAggr: tick1.name,
    winsize: _winsize, // 1 week (milliseconds)
    store: "Users"
});

// reads from a buffer
var gk1 = new qm.StreamAggr(base, {
    type: 'windowQuantiles',
    inAggr: winbuf1.name,
    quantileEps: quantileEps,
    countEps: countEps,
    quantiles: targetQuants,
    store: "Users"
});

// contains the pipeline
var set1 = new qm.StreamAggr(base, {
    type: "set",
    aggregates: [tick1.name, winbuf1.name, gk1.name],
    store: "Users"
});

// SET2
// reads from record, exposes a time series
let tick2 = new qm.StreamAggr (base, {
    type: "timeSeriesTick",
    store: "Users",
    timestamp: "Time",
    value: "TimeBetween"
});

// reads from a time series, exposes a buffer
let winbuf2 = new qm.StreamAggr(base, {
    type: "timeSeriesWinBufVector",
    inAggr: tick2.name,
    winsize: _winsize, // 1 week (milliseconds)
    store: "Users"
});

var gk2 = new qm.StreamAggr(base, {
    type: 'windowQuantiles',
    inAggr: winbuf2.name,
    quantileEps: quantileEps,
    countEps: countEps,
    quantiles: targetQuants,
    store: "Users"
});

// contains the pipeline
var set2 = new qm.StreamAggr(base, {
    type: "set",
    aggregates: [tick2.name, winbuf2.name, gk2.name],
    store: "Users"
});

//
// FILTERS
//
// select store with measurements and register two filter aggregates
// both will be triggered by every record, but only one of them will 
// trigger the rest of its pipeline (just the records that match its
// filter criteria
var users = base.store("Users");
// forwards records whose Id equals "segment1" to set1
var filter1 = users.addStreamAggr({
    type: 'recordFilterAggr',
    aggr: set1.name,
    filters: [{
        type: "field",
        store: "Users",
        field: "User",
        value: "0"
    }]
});

var filter2 = users.addStreamAggr({
    type: 'recordFilterAggr',
    aggr: set2.name,
    filters: [{
        type: "field",
        store: "Users",
        field: "User",
        value: "1"
    }]
});

// Create the anomaly detection aggregator
// let anomaly = users.addStreamAggr(aggrAD);

// Define monitoring stream aggregate.
// let monitoringAggr = users.addStreamAggr({
//     onAdd: (rec) => {        
//         if (anomaly.getInteger() > 2) console.log("Rate " + anomaly.getInteger() + ": " + rec["User"]);        
//     }
// });

// --------------------------
// read file and push stream
let times_file = qm.fs.openRead('D:/Data/telekom/features-subscriber/time-between-events-incoming.txt');

let header = times_file.readLine().split('\t');
console.log(header[0]);
console.log(header[4]);

let userResults = {};
for (i=1; i<3; i++) {
    let time = new Date(1475272800000);
    let line = times_file.readLine().split('\t');
    let user = line[0],
        times  = line[4];
    times = times.substring(1, times.length-2).split(' ');
    console.log('\n -----------------------------')
    console.log(`User: ${user}, number of events: ${times.length}`);
    
    let sum = 0;
    for (tsi=0; tsi<times.length-1; tsi++){ 
        sum += parseInt(times[tsi]);
    }
    console.log(`last event: ${new Date(1475272800000 + sum * 1000)}`);

    let results = [];
    let rates = []
    let count = 0;
    var gk;
    if (i==1) {
        gk = gk1;
    }
    else if (i==2) {
        gk = gk2;
    }
    for (tsi=0; tsi<times.length-1; tsi++){
        //calculate timestamp 
        time = new Date(time.getTime() + times[tsi]*1000);
        // push stream
        users.push({ 
            Time: time, 
            User: user, 
            TimeBetween: parseInt(times[tsi])
        });
        let result = gk.getFloatVector();
        // console.log(result.toString());
        if (parseInt(times[tsi]) <= result[1] || parseInt(times[tsi]) >= result[result.length-1]){
            debugger
            console.log(`Anomaly! Index: ${tsi}, ${new Date(time)}: ${times[tsi]}`);
        }
    }
    var result = gk.getFloatVector();
    console.log(`Result: ${gk.getFloatVector()}`);
}
