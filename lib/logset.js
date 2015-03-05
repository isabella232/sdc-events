// ---- log sets
var assert = require('assert-plus');
var path = require('path');
var fmt = require('util').format;

function LogSet(config) {
    assert.string(config.name, 'config.name');
    assert.optionalBool(config.global, 'config.global');
    if (!config.global) {
        assert.string(config.sapiSvcName, 'config.sapiSvcName');
    }
    assert.string(config.rottype, 'config.rottype');
    assert.string(config.rotdir, 'config.rotdir');
    assert.optionalString(config.rotname, 'config.rotname');

    for (var k in config) {
        this[k] = config[k];
    }
}

LogSet.prototype.getFileGlob = function getFileGlob(zone, hour) {
    assert.string(zone, 'zone');
    assert.string(hour, 'hour');

    var fileGlob;
    if (hour === 'curr') {
        fileGlob = this.curr;
    } else {
        assert.equal(this.rottype, 'sdc-hourly');
        fileGlob = path.join(this.rotdir,
            fmt('%s_*_%s*.log', this.rotname || this.name, hour));
    }
    if (zone !== 'global') {
        fileGlob = path.join('/zones', zone, 'root', fileGlob);
    }
    return fileGlob;
};

LogSet.prototype.toJSON = function toJSON() {
    return {
        name: this.name,
        global: Boolean(this.global),
        curr: this.curr
    };
};

module.exports = LogSet;
