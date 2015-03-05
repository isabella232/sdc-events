var stream = require('stream');
var util = require('util');
var fmt = util.fmt;

function TransBunyan2TraceEvent() {
    stream.Transform.call(this, {
        objectMode: true,
        // TODO: I don't understand the impact of this
        //highWaterMark: 0,
        encoding: 'utf8'
    });
}
util.inherits(TransBunyan2TraceEvent, stream.Transform);


module.exports = TransBunyan2TraceEvent;



TransBunyan2TraceEvent.prototype._transform = function (rec, enc, cb) {
    var ev = rec.evt;
    ev.pid = ev.tid = rec.pid;
    ev.id = rec.req_id || fmt('(no req_id %s)', genUuid());

    /*
     * Rebase all 'ts' to 0 because trace-viewer starts at zero, and scrolling
     * fwd from Jan 1, 1970 is pretty frustrating. :)
     *
     * TODO: option to reset per-id might make for nice above/below comparisons
     * in trace-viewer.
     */
    ev.ts = new Date(rec.time).valueOf() * 1000;
    if (!this._tsBase) {
        this._tsBase = ev.ts;
    }
    ev.ts -= this._tsBase;

    // TODO make prefixing to the <event>.name optional?
    // TODO add rec.component to the prefixing?
    ev.name = rec.name + '.' + ev.name;

    if (ev.cat) {
        ev.cat = rec.name + ',' + ev.cat;
    } else {
        ev.cat = rec.name;
    }

    // TODO consider adding req_id (or id) to the args, trace-viewer hides 'id'
    if (!ev.args) {
        ev.args = {};
    }

    if (!this._first) {
        this.push('[');
        this._first = true;
    } else {
        this.push(',\n');
    }
    this.push(JSON.stringify(ev));
    cb();
};

TransBunyan2TraceEvent.prototype._flush = function (cb) {
    if (this._first) {
        this.push(']\n');
    }
    cb();
};

