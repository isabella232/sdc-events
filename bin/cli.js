#!/usr/bin/env node
/*
 * TODO:
 * - check all LOGSETS are correct
 * - -j|-J|-b for bunyan output and tabular output.
 *   Option group showing all the output formats.
 * - -H, -o,  -o *,foo ('*' means default fields)
 * - -a; -n NODE,...; -n core, -N UUID  (node handling. see notes)
 * - `-s -imgapi` to *exclude* the imgapi logset
 * - shortcuts for logset groups, exclude heavy but uncommon ones (ufds?)
 *   by default?
 *
 * Someday/Maybe:
 * - `sdc-events ... vm=UUID` to first find all req_ids for ops on that VM
 *   in sdc-docker and cloudapi logs for that time period. Then get all
 *   events for those req_ids.
 * - `sdc-events ... owner=UUID` to first find all req_ids for ops for that
 *   user in sdc-docker and cloudapi logs for that time period. Then get all
 *   events for those req_ids.
 * - answer for `PROGRESS` func: want it without all trace logging, but probably
 *   not by default. So separate '-v' and TRACE envvar perhaps?
 * - follow
 * - --last: store last results' raw json stream to file based on PPID
 *   and allow re-access via --last. E.g. saw something interesting and want
 *   to see again.
 * - caching (cache "all events" for an hour and logset and use that)
 * - `-t TIME-RANGE`, e.g. `-t 3h-2h`
 */

var dashdash = require('dashdash');
var path = require('path');
var fs = require('fs');

var SDCEvents = require('../lib/index');

// ---- mainline

var OPTION_SPECS = [
    {
        names: [ 'help', 'h' ],
        type: 'bool',
        help: 'print this help message'
    },
    {
        names: [ 'version' ],
        type: 'bool',
        help: 'print the version'
    },
    {
        names: [ 'verbose', 'v' ],
        type: 'bool',
        help: 'verbose output'
    },
    {
        names: [ 'quiet', 'q' ],
        type: 'bool',
        help: 'quiet output'
    },
    {
        names: ['x'],
        type: 'arrayOfString',
        help: 'Internal testing option. Do not use this.'
    },
    {
        group: ''
    },
    {
        names: ['time', 't'],
        type: 'timeAgo',
        help: 'Start time. Specify a date or a time duration "ago", e.g. 2h ' +
            'for two hours ago (s=second, m=minute, h=hour, d=day). Default ' +
            'is one hour ago.'
    },
    {
        names: ['logset', 's'],
        type: 'arrayOfString',
        helpArg: 'NAME',
        help: 'Logsets to search. By default all logsets are searched. ' +
            'Known logsets: ' +
            SDCEvents.LOGSETS.map(function (ls) { return ls.name; }).sort().join(', ')
    },
    {
        names: ['event-trace', 'E'],
        type: 'bool',
        help: 'Output an event trace file, as required by trace-viewer ' +
            '<https://github.com/google/trace-viewer>. Note that this offsets' +
            'all times (the "ts" field) to zero for the first event to ' +
            'simplify finding the start in the viewer.'
    }
];


// ---- custom dashdash option type for `-t TIME`

/**
 * A 'timeAgo' option type that allows either a duration (an amount of time
 * ago):
 *      1h      one hour ago
 *      2d      two days ago
 *      90m     ninety minutes ago
 *      120s    120 seconds ago
 * or a date (another parsable by `new Date()`).
 */
var durationRe = /^([1-9]\d*)([smhd])$/;
function parseTimeAgo(option, optstr, arg) {
    var t;
    var match = durationRe.exec(arg);
    if (match) {
        var num = match[1];
        var scope = match[2];
        var delta = 0;
        switch (scope) {
            case 's':
                delta += num * 1000;
                break;
            case 'm':
                delta += num * 60 * 1000;
                break;
            case 'h':
                delta += num * 60 * 60 * 1000;
                break;
            case 'd':
                delta += num * 24 * 60 * 60 * 1000;
                break;
            default:
                throw new Error(fmt('unknown duration scope: "%s"', scope));
        }
        t = new Date(Date.now() - delta);
    } else {
        try {
            t = dashdash.parseDate(arg);
        } catch (ex) {
            throw new Error(fmt('arg for "%s" is not a valid duration ' +
                '(e.g. 1h) or date: "%s"', optstr, arg));
        }
    }
    return t;
}

// Here we add the new 'duration' option type to dashdash's set.
dashdash.addOptionType({
    name: 'timeAgo',
    takesArg: true,
    helpArg: 'TIME',
    parseArg: parseTimeAgo
});



function parseOpts(options, args) {
    var parser = dashdash.createParser({
        options: options,
        allowUnknown: false
    });

    function usage(msg) {
        var us = [
            'Usage:\n  sdc-events [<options>] [<req-id> ...]'
        ].join('\n') + '\n\nOptions:\n' + parser.help({
            indent: 2,
            headingIndent: 0
        });

        if (msg) {
            console.error('sdc-events error: ' + msg);
            console.error(us);
            process.exit(1);
        } else {
            console.log(us);
            process.exit(0);
        }
    }

    var opts;
    try {
        opts = parser.parse(args);
    } catch (ex) {
        usage(ex.message);
    }

    if (opts.help)
        usage();

    return (opts);
}


function readConfig() {
    var configPath = process.env.SMARTDC_CONFIG_FILE ||
        path.join(__dirname, '../etc/config.json');

    var obj;
    try {
        obj = JSON.parse(fs.readFileSync(configPath, 'utf8'));
    } catch (ex) {
        console.error('sdc-events error: could not read config file "%s": %s',
            configPath, ex);
        process.exit(1);
    }
    return (obj);
}

var debug = function () {};
if (process.env.TRACE) {
    debug = console.error;
}

function main() {
    var options = parseOpts(OPTION_SPECS, process.argv);
    if (options.verbose) {
        LOG.level('trace');
    }
    debug('options', options);

    PROGRESS = function () {};
    if (options.verbose) {
        PROGRESS = console.error;
    }


    var filters = [ ['evt', 'exists'] ];
    if (options._args.length > 0) {
        filters.push(['req_id', 'in', options._args]);
    }
    if (options.x) {
        // Hack internal option to override regular filtering. This can be
        // dangerous because it can result in large numbers of hits across
        // the DC.
        filters = [[options.x, 'raw']];
    }

    var config = readConfig();
    var sdcEvents = new SDCEvents({
        config: config,
        out: process.stdout
    });

    sdcEvents.search({
        filters: filters,
        time: options.time
    });
}


process.stdout.on('error', function (err) {
    if (err.code === 'EPIPE') {
        process.exit(0);
    }
});


main();
