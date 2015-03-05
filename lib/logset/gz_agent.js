var assert = require('assert-plus');
var util = require('util');
var LogSet = require('../logset');
var fmt = util.format;

function GzAgentLogSet(config) {
    assert.string(config.name, 'config.name');
    if (config.global === undefined) {
        config.global = true;
    }
    if (!config.curr) {
        config.curr = fmt('/var/svc/log/smartdc-agent-%s:default.log',
            config.name);
    }
    if (!config.rottype) {
        config.rottype = 'sdc-hourly';
    }
    if (!config.rotdir) {
        config.rotdir = fmt('/var/log/sdc/%s', config.name);
    }
    LogSet.call(this, config);
}
util.inherits(GzAgentLogSet, LogSet);


module.exports = GzAgentLogSet;
