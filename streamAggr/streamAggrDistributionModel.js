let qm = require('qminer');

class DistributionModel {

    constructor(opts){
        let self = this;

        if (opts.type == null) throw new Error('Parameter `type` missing');
        
        // non-serializable
        self._type = opts.type;

        self._model = null;
    }

    static create(opts) {
        console.log(`Creating distribution estimator: ${opts.type}`);
        let type = opts.type;

        switch (type) {
            case 'gk': {
                return new GkDistributionModel(opts);
            }
            default: {
                throw new Error(`Unknown model type: ${type}`);
            }
        }
    }
}

class GkDistributionModel extends DistributionModel {

    constructor(opts){
        super(opts);
        
        let self = this;

        let settings = opts;
        if (settings.qeps == null) throw new Error('GK: parameter `qeps` missing!');
        if (settings.ceps == null) throw new Error('GK: parameter `ceps` missing!');
        if (settings.tarQuants == null) throw new Error('GK: parameter `tarQuants` missing!');
        if (settings.win == null) throw new Error('GK: parameter `win` missing!');
        if (settings.schemaName == null) throw new Error('GK: parameter `schemaName` missing!');


        self._qeps = settings.qeps;
        self._ceps = settings.ceps;
        self._tarQuants = settings.tarQuants;
        self._win = settings.win;
        self._schemaName = settings.schemaName;

        self._streamIds = [];
        self._gk = {};

        this.createSchema(settings.schemaName);
    }

    get gk() {
        return this._gk;
    }

    getGk(streamId){
        return this._gk[streamId];
    }

    addSet(streamId){
        if (!streamId) throw new Error('Parameter `streamId` missing!');
        if (this._streamIds.includes(streamId)) 
            throw new Error('This streamId already exists!');

        let self = this;
        self._streamIds.push(streamId);
        self.createPipeline(streamId);
    }

    createSchema(schemaName){
        var base = new qm.Base({
            mode: 'createClean',
            schema:[
                {
                    name: schemaName,
                    fields: [
                        { name: 'Time', type: 'datetime' },
                        { name: 'User', type: 'string' },
                        { name: 'TimeBetween', type: 'int' }
                    ]
                }
            ]
        });
        this._schema = base.store(schemaName);
    }

    createPipeline(streamId){
        // reads from record, exposes a time series
        let tick = this._schema.addStreamAggr({
            name: "tick" + streamId,
            type: "timeSeriesTick",
            store: this._schemaName,
            timestamp: "Time",
            value: "TimeBetween"
        });
        
        // reads from a time series, exposes a buffer
        let winbuf = this._schema.addStreamAggr({
            name: "winbuf" + streamId,
            type: "timeSeriesWinBufVector",
            inAggr: tick.name,
            winsize: this._win, 
            store: this._schemaName
        });
        
        // reads from a buffer
        let gk = this._schema.addStreamAggr({
            name: "gk" + streamId,
            type: 'windowQuantiles',
            inAggr: winbuf.name,
            quantileEps: this._qeps,
            countEps: this._ceps,
            quantiles: this._tarQuants,
            store: this._schemaName
        });
        this._gk[streamId] = gk;
        
        // contains the pipeline
        let set = this._schema.addStreamAggr({
            type: "set",
            aggregates: [tick.name, winbuf.name, gk.name],
            store: this._schemaName
        });

        // forwards records whose Id equals streamId to set
        var filter = this._schema.addStreamAggr({
            type: 'recordFilterAggr',
            aggr: set.name,
            filters: [{
                type: "field",
                store: this._schemaName,
                field: "User",
                value: streamId
            }]
        });
    }

}

exports.DistributionModel = DistributionModel;
