/* vim: syn=javascript ts=4 sts=4 sw=4 et: */

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2015, Joyent, Inc.
 */

/*
 * List/tail SDC events.
 *
 * Effectively, "events" are bunyan log records from any SDC service with
 * a standard "evt" field.
 * TODO: explain our events plan
 *
 * Typical usage is for getting timings
 * of tasks. Commonly these are coupled with "req_id" at the top-level to
 * group start and end events.
 *
 * * *
 *
 * Listing events means grepping log files. Well-known log file locations
 * are hardcoded here and grouped in "logsets".
 */


var VERSION = '1.1.0';

var assert = require('assert-plus');
var bunyan = require('bunyan');
var child_process = require('child_process');
var fs = require('fs');
var os = require('os');
var path = require('path');
var sdcClients = require('sdc-clients');
var spawn = require('child_process').spawn;
var vasync = require('vasync');
var VError = require('verror').VError;
var urclient = require('urclient');
var util = require('util');
var fmt = util.format;


// ---- globals

var debug = function () {};
if (process.env.TRACE) {
    debug = console.error;
}
var p = console.error; // for dev use, don't commit with this used

/*
 * Unfortunately, bunyan does not presently have an output stream that can emit
 * pre-formatted messages to stderr -- see: node-bunyan#13 and node-bunyan#102.
 * For now, we shall keep bunyan logging for debugging purposes and emit our
 * own human-readable messages in verbose mode.
 */


var LogSet = require('./logset');
var ZoneLogSet = require('./logset/zone');
var GzAgentLogSet = require('./logset/gz_agent');


var DEFAULT_LOGSETS = [
    new ZoneLogSet({name: 'imgapi'}),
    new ZoneLogSet({name: 'napi'}),
    new ZoneLogSet({name: 'cnapi'}),
    new ZoneLogSet({name: 'vmapi'}),
    new ZoneLogSet({
        name: 'docker',
        curr: '/var/svc/log/smartdc-application-docker:default.log'
    }),
    new ZoneLogSet({name: 'sapi'}),
    new ZoneLogSet({name: 'papi'}),
    new ZoneLogSet({name: 'fwapi'}),
    new ZoneLogSet({name: 'amon-master', sapiSvcName: 'amon'}),
    new ZoneLogSet({
        name: 'wf-api',
        sapiSvcName: 'workflow',
        curr: '/var/svc/log/smartdc-application-wf-api:default.log'
    }),
    new ZoneLogSet({
        name: 'wf-runner',
        sapiSvcName: 'workflow',
        curr: '/var/svc/log/smartdc-application-wf-runner:default.log'
    }),

    new LogSet({
        name: 'cloudapi',
        sapiSvcName: 'cloudapi',
        curr: '/var/svc/log/smartdc-application-cloudapi:cloudapi-*.log',
        rottype: 'sdc-hourly',
        rotdir: '/var/log/sdc/upload',
        rotname: 'cloudapi-*'
    }),

    new LogSet({
        name: 'ufds-master',
        sapiSvcName: 'ufds',
        curr: '/var/svc/log/smartdc-application-ufds-master:ufds-*.log',
        rottype: 'sdc-hourly',
        rotdir: '/var/log/sdc/upload',
        rotname: 'ufds-master-*'
    }),

    new GzAgentLogSet({name: 'vm-agent'}),
    new GzAgentLogSet({name: 'net-agent'}),
    new GzAgentLogSet({name: 'firewaller', rotdir: '/var/log/sdc/upload'}),

    new GzAgentLogSet({name: 'cn-agent'}),
    new LogSet({
        name: 'cn-agent-tasks',
        global: true,
        curr: '/var/log/cn-agent/logs/*.log',
        rottype: 'sdc-hourly',
        rotdir: '/var/log/cn-agent'
    }),

    new GzAgentLogSet({name: 'provisioner'}),
    new LogSet({
        name: 'provisioner-tasks',
        global: true,
        curr: '/var/log/provisioner/logs/*.log',
        rottype: 'sdc-hourly',
        rotdir: '/var/log/provisioner',
        rotname: 'provisioner_tasks'
    }),

    new LogSet({
        name: 'vmadm',
        global: true,
        curr: '/var/log/vm/logs/*.log',
        rottype: 'sdc-hourly',
        rotdir: '/var/log/vm'
    }),
    new LogSet({
        name: 'vmadmd',
        global: true,
        curr: '/var/svc/log/system-smartdc-vmadmd:default.log',
        rottype: 'sdc-hourly',
        rotdir: '/var/log/vm'
    }),
    new LogSet({
        name: 'fwadm',
        global: true,
        curr: '/var/log/fw/logs/*.log',
        rottype: 'sdc-hourly',
        rotdir: '/var/log/fw'
    })

    /*
     * TODO: Other logs to consider:
     * - hagfish-watcher gz agent doesn't rotate
     * - hermes-actor gz agent doesn't rotate
     * - config-agent gz agent doesn't rotate
     * - smartlogin gz agent doesn't rotate
     * - metadata gz agent doesn't rotate
     * - gz amon-agent and amon-relay don't rotate
     * - ur gz agent doesn't rotate
     * - sdc: hermes and hermes-proxy?
     * - dhcpd?
     * - binder?
     * - mahi?
     * - adminui?
     * - ca? and cainstsvc gz agent?
     * - zones' amon-agent and config-agent? and registrar?
     *
     * Excluded logs:
     * - heartbeater doesn't rotate (deprecated agent, so leave this out)
     */
];



// ---- internal support stuff

function humanDurationFromMs(ms) {
    assert.number(ms, 'ms');
    var sizes = [
        ['ms', 1000, 's'],
        ['s', 60, 'm'],
        ['m', 60, 'h'],
        ['h', 24, 'd']
    ];
    if (ms === 0) {
        return '0ms';
    }
    var bits = [];
    var n = ms;
    for (var i = 0; i < sizes.length; i++) {
        var size = sizes[i];
        var remainder = n % size[1];
        if (remainder === 0) {
            bits.unshift('');
        } else {
            bits.unshift(fmt('%d%s', remainder, size[0]));
        }
        n = Math.floor(n / size[1]);
        if (n === 0) {
            break;
        } else if (size[2] === 'd') {
            bits.unshift(fmt('%d%s', n, size[2]));
            break;
        }
    }
    return bits.slice(0, 2).join('');
}


function getLocalIpSync(config) {
    var interfaces = os.networkInterfaces();
    var ifs = interfaces.net0 || interfaces.en1 || interfaces.en0;
    var ip;

    /*
     * Not running inside 'sdc' zone
     */
    if (!ifs) {
        return (config.admin_ip);
    }

    for (var i = 0; i < ifs.length; i++) {
        if (ifs[i].family === 'IPv4') {
            ip = ifs[i].address;
            break;
        }
    }
    return ip;
}

function getAmqpConfigSync(config) {
    assert.object(config, 'config');
    assert.string(config.rabbitmq, 'config.rabbitmq');

    var arr = config.rabbitmq.split(':');
    assert.strictEqual(arr.length, 4, 'malformed rabbitmq: ' +
        config.rabbitmq);

    return ({
        login: arr[0],
        password: arr[1],
        host: arr[2],
        port: Number(arr[3])
    });
}





var BunyanSortedGrep = require('./bunyan_sorted_grep');



// ---- renderers

var TransObj2JsonStream = require('./transform/obj_to_json_stream');
var TransBunyan2TraceEvent = require('./transform/bunyan_to_trace');

/**
 * [SDCEvents description]
 * @param {object} opts.config Object containing smartdc_config
 */
function SDCEvents(opts) {
    this.opts = opts;
    this.config = opts.config;
    this.zonename = opts.zonename;
    this.out = opts.out;
    if (opts.log) {
        this.log = opts.log;
    } else {
        this.log = bunyan.createLogger({
            level: 'warn',
            name: 'sdc-events',
            stream: process.stderr
        });
    }
    this._initSapi();
    this._initVmapi();
    this._initCnapi();
}

module.exports = SDCEvents;

SDCEvents.LOGSETS = DEFAULT_LOGSETS;


/**
 * performs a log search
 * @param  {[object]} searchOptions [description]
 * @return {[array]} searchOptions.filters
 * @return {[Date]} searchOptions.time (start search from this date/time)
 */
SDCEvents.prototype.search = function (searchOptions) {
    var self = this;
    var CONFIG = this.config;
    var LOG = this.log;
    var OPTIONS = this.opts;
    var CNAPI = this.CNAPI;

    var oneHour = 60 * 60 * 1000;
    var now = Date.now();
    var start = searchOptions.time || new Date(now - oneHour);

    // Ensure we don't try to search a huge time range.
    var MAX_RANGE = 7 * 24 * oneHour; // one week
    var range = now - start;
    if (range > MAX_RANGE) {
        throw new Error(fmt('time range, %s, is too large (>%s)',
            humanDurationFromMs(range), humanDurationFromMs(MAX_RANGE)));
    }

    var filters = searchOptions.filters || [];
    if (!filters.length) {
        throw new Error('no filters provided to search');
    }

    vasync.pipeline({arg: {}, funcs: [
        function getZonename(ctx, next) {
            if (self.zonename) {
                ctx.zonename = self.zonename;
                next();
            } else {
                child_process.execFile('/usr/bin/zonename', [], {},
                        function (err, stdout, stderr) {
                    if (err) {
                        return next(err);
                    }
                    ctx.zonename = stdout.trim();
                    return next();
                });
            }
        },

        function getNodes(ctx, next) {
            CNAPI.listServers(function (err, servers) {
                ctx.nodes = servers.filter(function (server) {
                    return server.status === 'running' && server.setup;
                });
                ctx.nodeFromUuid = [];
                ctx.nodeFromHostname = [];
                for (var i = 0; i < ctx.nodes.length; i++) {
                    var node = ctx.nodes[i];
                    ctx.nodeFromUuid[node.uuid] = node;
                    ctx.nodeFromHostname[node.hostname] = node;
                }
                next();
            });
        },

        function getLogsets(ctx, next) {
            if (!OPTIONS.logset || OPTIONS.logset.length === 0) {
                ctx.logsets = DEFAULT_LOGSETS;
            } else {
                ctx.logsets = [];
                var logsetFromName = {};
                for (var i = 0; i < DEFAULT_LOGSETS.length; i++) {
                    logsetFromName[DEFAULT_LOGSETS[i].name] =
                        DEFAULT_LOGSETS[i];
                }
                for (i = 0; i < OPTIONS.logset.length; i++) {
                    var name = OPTIONS.logset[i];
                    if (!logsetFromName[name]) {
                        return next(new Error(
                            fmt('unknown logset: "%s"', name)));
                    }
                    ctx.logsets.push(logsetFromName[name]);
                }
            }
            next();
        },

        function getSdcInsts(ctx, next) {
            self._sapiGetInstances({
                app: 'sdc'
            }, function (err, insts, instsFromSvc) {
                if (err) {
                    return next(err);
                }
                ctx.sdcInstsFromSvc = instsFromSvc;
                ctx.sdcInstFromUuid = {};
                for (var i = 0; i < insts.length; i++) {
                    ctx.sdcInstFromUuid[insts[i].uuid] = insts[i];
                }
                return next();
            });
        },

        function getVmInfo(ctx, next) {
            /**
             * Instead of getting each VM (there could be up to dozens),
             * lets get all of admin's VMs in one req and filter those.
             *
             * 'cloudapi' zones typically don't have
             * `tags.smartdc_core=true` so we can't filter on that. And
             * VMAPI doesn't support filtering on presence of a tag
             * (e.g. `smartdc_role`).
             */
            ctx.vmFromUuid = {};
            var filter = {
                state: 'active',
                owner_uuid: CONFIG.ufds_admin_uuid
            };
            self.VMAPI.listVms(filter, function (err, vms) {
                if (err) {
                    return next(err);
                }
                for (var i = 0; i < vms.length; i++) {
                    var vm = vms[i];
                    if (ctx.sdcInstFromUuid[vm.uuid]) {
                        ctx.vmFromUuid[vm.uuid] = vm;
                    }
                }
                return next();
            });
        },

        function getLogInsts(ctx, next) {
            var i, j;
            ctx.logInsts = [];
            ctx.haveNonHeadnodeInsts = false;
            for (i = 0; i < ctx.logsets.length; i++) {
                var logset = ctx.logsets[i];
                if (logset.global) {
                    for (j = 0; j < ctx.nodes.length; j++) {
                        if (!ctx.nodes[j].headnode) {
                            ctx.haveNonHeadnodeInsts = true;
                        }
                        ctx.logInsts.push({
                            logset: logset,
                            node: ctx.nodes[j],
                            zone: 'global'
                        });
                    }
                } else {
                    var sdcInsts = ctx.sdcInstsFromSvc[
                        logset.sapiSvcName] || [];
                    for (j = 0; j < sdcInsts.length; j++) {
                        var nodeUuid =
                            ctx.vmFromUuid[sdcInsts[j].uuid].server_uuid;

                        var node = ctx.nodeFromUuid[nodeUuid];
                        if (node) {
                            if (!node.headnode) {
                                ctx.haveNonHeadnodeInsts = true;
                            }
                            ctx.logInsts.push({
                                logset: logset,
                                node: node,
                                zone: sdcInsts[j].uuid
                            });
                        }
                    }
                }
            }
            next();
        },

        function initUrClientIfNeeded(ctx, next) {
            if (ctx.zonename === 'global' && !ctx.haveNonHeadnodeInsts) {
                return next();
            }

            self.URCLIENT = urclient.create_ur_client({
                log: self.log,
                connect_timeout: 5000,
                enable_http: false,
                bind_ip: getLocalIpSync(CONFIG),
                amqp_config: getAmqpConfigSync(CONFIG)
            });
            self.URCLIENT.on('ready', next);
            return null; // keep linter happy
        },

        function chooseRenderer(ctx, next) {
            if (OPTIONS.event_trace) {
                ctx.renderer = new TransBunyan2TraceEvent();
            } else {
                ctx.renderer = new TransObj2JsonStream();
            }
            next();
        },

        function searchByHour(ctx, next) {
            // Limitation: Assuming `logset.rottype == 'sdc-hourly'`.
            var hours = [];
            var topOfHour = now - (now % oneHour);
            // Offset *forward* one hour because logs starting at, e.g.,
            // 2015-02-13T20:15:03 are in this log file:
            // "${logset.name}_*_2015-02-13T21:*.log"
            var s = start.valueOf();
            while (s <= topOfHour) {
                hours.push(new Date(s + oneHour).toISOString().slice(0, 14));
                s += oneHour;
            }
            hours.push('curr');
            LOG.info({now: new Date(now), start: start, hours: hours},
                'hours');

            LOG.debug('Searching %d logsets across %d nodes (%d insts), ' +
                'in %d one hour segments', ctx.logsets.length, ctx.nodes.length,
                ctx.logInsts.length, hours.length);

            ctx.renderer.pipe(OPTIONS.out);

            vasync.forEachPipeline({
                inputs: hours,
                func: function searchOneHour(hour, nextHour) {
                    LOG.trace('Searching hour "%s"', hour);
                    var hits = new BunyanSortedGrep({
                        urclient: self.URCLIENT,
                        zonename: ctx.zonename,
                        hour: hour,
                        logInsts: ctx.logInsts,
                        filters: filters,
                        startTimeCut: (hour === hours[0] ? start : undefined)
                    });
                    hits.pipe(ctx.renderer, {end: false});
                    hits.on('end', function () {
                        nextHour();
                    });
                }
            }, next);
        },

        function closeThings(ctx, next) {
            if (self.URCLIENT) {
                self.URCLIENT.close();
            }
            ctx.renderer.end();
            next();
        }

    ]}, function done(err) {
        if (err) {
            console.error('sdc-events error: %s',
                (OPTIONS.verbose ? err.stack : err.message));
            process.exit(1);
        }
    });
};

SDCEvents.prototype._initCnapi = function initCnapi() {
    assert.string(this.config.cnapi_domain, 'config.cnapi_domain');

    this.CNAPI = new sdcClients.CNAPI({
        log: this.log.child({component: 'cnapi'}, true),
        url: 'http://' + this.config.cnapi_domain,
        agent: false
    });
};

SDCEvents.prototype._initSapi = function initSapi() {
    assert.string(this.config.sapi_domain, 'config.sapi_domain');

    this.SAPI = new sdcClients.SAPI({
        log: this.log.child({component: 'sapi'}, true),
        url: 'http://' + this.config.sapi_domain,
        agent: false
    });
};

SDCEvents.prototype._initVmapi = function initVmapi() {
    assert.object(this.log, 'log');
    assert.object(this.config, 'config');
    assert.string(this.config.vmapi_domain, 'config.vmapi_domain');

    this.VMAPI = new sdcClients.VMAPI({
       log: this.log.child({component: 'vmapi'}, true),
       url: 'http://' + this.config.vmapi_domain,
       agent: false
    });
};




/**
 * It is a bit of a PITA to get the set of instances for a single app
 * in SDC, e.g. getting all the 'sdc' instances when the 'manta' app is
 * in the mix.
 */
SDCEvents.prototype._sapiGetInstances = function _sapiGetInstances(opts, cb) {
    assert.object(opts, 'opts');
    assert.string(opts.app, 'opts.app');
    assert.func(cb, 'cb');
    var SAPI = this.SAPI;

    SAPI.listApplications({name: opts.app}, function (appsErr, apps) {
        if (appsErr) {
            return cb(appsErr);
        } else if (apps.length !== 1) {
            return cb(new Error(fmt('unexpected number of "%s" apps: %d',
                opts.app, apps.length)));
        }
        var appUuid = apps[0].uuid;

        return SAPI.listServices({
            application_uuid: appUuid
        }, function (err, svcs) {
            if (err) {
                return cb(err);
            }
            var svcFromUuid = {};
            var instsFromSvcName = {};
            svcs.forEach(function (svc) {
                svcFromUuid[svc.uuid] = svc;
                instsFromSvcName[svc.name] = [];
            });

            return SAPI.listInstances(function (instsErr, allInsts) {
                if (instsErr) {
                    return cb(instsErr);
                }
                var insts = [];
                for (var i = 0; i < allInsts.length; i++) {
                    var inst = allInsts[i];
                    var svc = svcFromUuid[inst.service_uuid];
                    if (svc) {
                        inst.svc = svc;
                        insts.push(inst);
                        instsFromSvcName[svc.name].push(inst);
                    }
                }
                return cb(null, insts, instsFromSvcName);
            });
        });
    });
};
