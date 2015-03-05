var assert = require('assert-plus');
var util = require('util');
var LogSet = require('../logset');
var fmt = util.format;

function ZoneLogSet(config) {
    assert.string(config.name, 'config.name');
    if (!config.sapiSvcName) {
        config.sapiSvcName = config.name;
    }
    if (!config.curr) {
        config.curr = fmt('/var/svc/log/smartdc-site-%s:default.log',
            config.name);
    }
    if (!config.rottype) {
        config.rottype = 'sdc-hourly';
    }
    if (!config.rotdir) {
        config.rotdir = '/var/log/sdc/upload';
    }
    LogSet.call(this, config);
}
util.inherits(ZoneLogSet, LogSet);

module.exports = ZoneLogSet;
