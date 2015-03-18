/**
 * Grep the bunyan log for each given "log instance" (logInst) for a one hour
 * segment, then sort the results chronologically. This creates a new
 * readable stream of hits. The returned hits are the raw bunyan log line
 * (i.e. a string).
 */

var assert = require('assert-plus');
var vasync = require('vasync');
var util = require('util');
var fmt = util.format;
var stream = require('stream');
var bunyan = require('bunyan');

function BunyanSortedGrep(opts) {
    assert.object(opts, 'opts');


    assert.string(opts.zonename, 'opts.zonename');
    assert.string(opts.hour, 'opts.hour');
    assert.arrayOfObject(opts.logInsts, 'opts.logInsts');
    assert.arrayOfObject(opts.filters, 'opts.filters');
    assert.optionalObject(opts.startTimeCut, 'opts.startTimeCut');

    if (opts.log) {
        this.log = opts.log.child({
            level: 'debug',
            name: 'sdc-events/bunyan-sorted-grep'
        });
    } else {
        this.log = bunyan.createLogger({
            level: 'debug',
            name: 'sdc-events/bunyan-sorted-grep'
        });
    }

    this.log.debug({opts: opts}, 'BunyanSortedGrep');



    this.URCLIENT = opts.urclient;
    this.zonename = opts.zonename;
    this.hour = opts.hour;
    this.logInsts = opts.logInsts;
    this.startTimeCut = opts.startTimeCut;

    /*
     * `filters` is an array of filter definitions like this:
     *      [<field>, <op>[, <value>]]
     *
     * Supported <op>s are (shown with examples):
     *      ['TERM', 'raw']
     *          grep for 'TERM'
     *      ['evt', 'exists']
     *          'evt' field exists
     *      ['req_id', 'in', [<UUID1>, <UUID2>]]
     *          'req_id' field is one of the given UUIDs
     *
     * These get translated to grep patterns.
     *
     * TODO: We should also do post-filtering on the pre-`grep`d and
     * `JSON.parse`d Bunyan records to avoid false positives.
     */
    this.grepPatterns = [];
    for (var i = 0; i < opts.filters.length; i++) {
        var field = opts.filters[i][0];
        var op = opts.filters[i][1];
        var value = opts.filters[i][2];
        switch (op) {
        case 'raw':
            this.grepPatterns.push(fmt('%s', field));
            break;
        case 'exists':
            this.grepPatterns.push(fmt('"%s":', field));
            break;
        case 'in':
            // Only support string values for now.
            assert.arrayOfString(value, 'opts.filters['+i+'][2]');
            this.grepPatterns.push(fmt('"%s":"(%s)"', field, value.join('|')));
            break;
        default:
            throw new Error(fmt(
                'unknown BunyanSortedGrep filter op: "%s"', op));
        }
    }

    stream.Readable.call(this, {objectMode: true});
}
util.inherits(BunyanSortedGrep, stream.Readable);


BunyanSortedGrep.prototype._localGrep = function _localGrep(logInst, cb) {
    var fileGlob = logInst.logset.getFileGlob(logInst.zone, this.hour);
    assert.equal(this.grepPatterns.join('\n').indexOf('\''), -1,
        'Limitation: not escaping single-quotes yet');
    var grepCmd = '';
    for (var i = 0; i < this.grepPatterns.length; i++) {
        if (i === 0) {
            grepCmd += fmt('/usr/bin/egrep -h -- \'%s\' %s',
                this.grepPatterns[i], fileGlob);
        } else {
            grepCmd += fmt(' | /usr/bin/egrep -- \'%s\'', this.grepPatterns[i]);
        }
    }
    var argv = ['/usr/bin/bash', '-c', grepCmd];
    this.log.debug('argv: %j', argv);

    var grep = spawn(argv[0], argv.slice(1),
        {stdio: ['ignore', 'pipe', 'ignore']});
    grep.stdout.setEncoding('utf8');
    grep.on('error', function (err) {
        self.log.error('ERROR: _localGrep error:', err);
    });

    var chunks = [];
    grep.stdout.on('data', function (chunk) {
        chunks.push(chunk);
    });
    grep.on('close', function () {
        cb(null, chunks.join(''));
    });

    // Dev Note: perhaps useful when streaming
    //var lstream = new LineStream({encoding: 'utf8'});
    //lstream.on('error', onGrepError);
    //lstream.on('line', onGrepHit);
    //lstream.on('finish', onGrepFinish);
    //grep.stdout.pipe(lstream);
};


BunyanSortedGrep.prototype._urGrep = function _urGrep(logInst, cb) {
    var fileGlob = logInst.logset.getFileGlob(logInst.zone, this.hour);

    assert.equal(this.grepPatterns.join('\n').indexOf('\''), -1,
        'Limitation: not escaping single-quotes yet');
    var grepCmd = '';
    for (var i = 0; i < this.grepPatterns.length; i++) {
        if (i === 0) {
            grepCmd += fmt('/usr/bin/egrep -h -- \'%s\' "${file}"',
                this.grepPatterns[i]);
        } else {
            grepCmd += fmt(' | /usr/bin/egrep -- \'%s\'', this.grepPatterns[i]);
        }
    }

    var script = [
        '#!/bin/bash',
        '',
        'for file in ' + fileGlob + '; do',
        '    if [[ -f "${file}" ]]; then',
        '        ' + grepCmd,
        '    fi',
        'done',
        'exit 0'
    ].join('\n');

    this.URCLIENT.exec({
        script: script,
        server_uuid: logInst.node.uuid,
        timeout: 30 * 1000,
        env: {}
    }, function (err, result) {
        if (err) {
            // TODO: just warn?
            cb(err);
        } else if (result.exit_status !== 0) {
            cb(new Error(fmt('error running grep on server "%s": %s',
                logInst.node.uuid, result.stderr)));
        } else {
            // TODO: How do we tell if the output is clipped?
            cb(null, result.stdout);
        }
    });
};


BunyanSortedGrep.prototype._start = function () {
    var self = this;
    var hits = [];

    var queue = vasync.queuev({
        concurrency: 5,
        worker: grepOneInst
    });
    queue.on('end', doneGreps);
    queue.push(self.logInsts, function doneOneInst(err) {
        if (err) {
            // TODO: gracefully handle this
            throw err;
        }
    });
    queue.close();

    function grepOneInst(logInst, next) {
        var grepFunc = (self.zonename === 'global' && logInst.node.headnode ? '_localGrep' : '_urGrep');
        self[grepFunc](logInst, function (err, output) {
            if (err) {
                return next(err);
            } else if (!output) {
                return next();
            }
            var rec;
            var lines = output.split(/\n/);
            for (var i = 0; i < lines.length; i++) {
                var line = lines[i];
                if (!line.trim()) {
                    continue;
                }
                try {
                    rec = JSON.parse(line);
                } catch (ex) {
                    self.log.warn('WARN: grep hit is not a JSON line (skip): %j',
                        line);
                    continue;
                }

                var time = new Date(rec.time);
                if (self.startTimeCut && time < self.startTimeCut) {
                    continue;
                }

                hits.push({
                    line: line,
                    rec: rec,
                    time: time
                });
            }

            next();
        });
    }

    function doneGreps() {
        // Done receiving hits: sort and push them.
        var SORT_START = Date.now();
        self.log.debug('[%s] start sorting %d hits for hour "%s"',
            SORT_START, hits.length, self.hour);
        hits = hits.sort(function cmpTime(a, b) {
            if (a.time < b.time) {
                return -1;
            } else if (a.time > b.time) {
                return 1;
            } else {
                return 0;
            }
        });
        var SORT_END = Date.now();
        self.log.debug('[%s] end sorting %d hits for hour "%s" (duration %s)',
            SORT_END, hits.length, self.hour, SORT_END-SORT_START);

        for (var i = 0; i < hits.length; i++) {
            if (!self.push(hits[i].rec)) {
                self.log.warn('WARN: ignoring backpressure!');
            }
        }
        self.push(null);
    }
};

BunyanSortedGrep.prototype._read = function (size) {
    if (!this._started) {
        this._started = true;
        this._start();
    }
};


module.exports = BunyanSortedGrep;
