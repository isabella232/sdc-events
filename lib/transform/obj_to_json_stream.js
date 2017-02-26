/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/*
 * Copyright (c) 2017, Joyent, Inc.
 */

var util = require('util');
var fmt = util.fmt;
var stream = require('stream');


function TransObj2JsonStream() {
    stream.Transform.call(this, {
        /* BEGIN JSSTYLED */
        /*
         * TODO: I don't understand the impact of this. Setting to
         * highWaterMark=0 plus 1000s of hits results in:
         *      Trace: (node) warning: Recursive process.nextTick detected. This will break in the next version of node. Please use setImmediate for recursive deferral.
         *           at maxTickWarn (node.js:381:17)
         *           at process._nextTick [as _currentTickHandler] (node.js:484:9)
         *           at process.nextTick (node.js:335:15)
         *           at onwrite (_stream_writable.js:266:15)
         *           at WritableState.onwrite (_stream_writable.js:97:5)
         *           at WriteStream.Socket._write (net.js:653:5)
         *           at doWrite (_stream_writable.js:226:10)
         *           at writeOrBuffer (_stream_writable.js:216:5)
         *           at WriteStream.Writable.write (_stream_writable.js:183:11)
         *           at WriteStream.Socket.write (net.js:615:40)
         *           at Console.warn (console.js:61:16)
         *           at Console.trace (console.js:95:8)
         * and a crash on recursion limit. See related discussion at
         * <https://github.com/joyent/node/issues/6718>.
         */
        //highWaterMark: 0,
        /* END JSSTYLED */
        objectMode: true
    });
}
util.inherits(TransObj2JsonStream, stream.Transform);



module.exports = TransObj2JsonStream;

TransObj2JsonStream.prototype._transform = function (chunk, enc, cb) {
    this.push(JSON.stringify(chunk) + '\n');
    cb();
};
