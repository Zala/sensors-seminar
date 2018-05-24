// this version of anomaly detection using stream aggregates doesn't work well 
// because the algorithm doesn't handle well the ties at the tail of 
// distribution. 
// E.g. [100 4 150 200 4 200] will report both 4 and 200 as anomalies


const qm = require("qminer");
var distributions = require('./streamAggrDistributionModel.js');

var _winsize = 4 * 7 * 24 * 3600 * 1000;

var quantileEps = 0.01;
var countEps = 0.001;
var maxRelErr = quantileEps + 2*countEps;
console.log('Quantile granularity: 0.01')
var targetQuants = (function () {
    var quants = [];
    for (var prob = 0; prob <= 1; prob += 0.001) {
        quants.push(prob);
    }
    return quants;
})();

// create model
let settings = {
    type: 'gk',
    qeps: quantileEps,
    ceps: countEps,
    tarQuants: targetQuants,
    win: _winsize,
    schemaName: 'Users'
};
let gkModel = distributions.DistributionModel.create(settings);

// --------------------------
// read file and push stream
let times_file = qm.fs.openRead('D:/Data/telekom/features-subscriber/time-between-events-incoming.txt');

let header = times_file.readLine().split('\t');
console.log(header[0]);
console.log(header[4]);

let userResults = {};
for (i=1; i<10; i++) {
    let time = new Date(1475272800000);
    let line = times_file.readLine().split('\t');
    let user = line[0],
        times  = line[4];
    times = times.substring(1, times.length-2).split(' ');
    console.log('\n -----------------------------')
    console.log(`User: ${user}, number of events: ${times.length}`);
    
    gkModel.addSet(user);
    let sum = 0;
    for (tsi=0; tsi<times.length-1; tsi++){ 
        sum += parseInt(times[tsi]);
    }
    console.log(`last event: ${new Date(1475272800000 + sum * 1000)}`);

    let results = [];
    let rates = []
    let count = 0;
    for (tsi=0; tsi<times.length-1; tsi++){
        //calculate timestamp 
        time = new Date(time.getTime() + times[tsi]*1000);
        // push stream
        gkModel._schema.push({ 
            Time: time, 
            User: user, 
            TimeBetween: parseInt(times[tsi])
        });
        let result = gkModel.getGk(user).getFloatVector();
        if (time > new Date(1475272800000 + _winsize).getTime()){
            if (parseInt(times[tsi]) <= result[2] || parseInt(times[tsi]) >= result[result.length-2]){
                console.log(`Anomaly! Index: ${tsi}, ${new Date(time)}: ${times[tsi]}`);
            }
        }
    }
}
