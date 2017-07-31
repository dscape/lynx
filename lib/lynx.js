var dgram    = require('dgram')
  , Stream   = require('stream').Stream
  , util     = require('util')
  , parser   = require('statsd-parser')
  //
  // `Math.random` doesn't cut it, based on tests from sampling.js
  // Variations are wild for large data sets
  //
  , mersenne = require('mersenne')
  , mt       = new mersenne.MersenneTwister19937()
  , noop     = function noop() {}
  ;

function makeError(opts) {
  var error = new Error(opts.message);
  error.f = opts.f;
  error.args = opts.args;
  return error;
}

//
// Max idle time for a ephemeral socket
//
var EPHEMERAL_LIFETIME_MS = 1000;

//
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ constructors ~~
//

//
// ### constructor Lynx(host, port, socket)
// #### @host    {String} Server host name
// #### @port    {Number} Server port
// #### @options {Object} Aditional options
// #### @options.socket   {Object} Optional socket if we want to share
// #### @options.on_error {Function} A function to execute on errors
// #### @options.scope    {String} define the a prefix for all stats, 
//      e.g. with `scope` 'product1' and stat 'somestat' the key would 
//      actually be 'product1.somestat'.
//
// var client = new lynx('localhost', 8125);
//
// Returns a new `Lynx` client
//
function Lynx(host, port, options) {
  if (!(this instanceof Lynx)) {
    return new Lynx(host, port, options);
  }
  
  var self = this;

  //
  // Server hostname and port
  //
  this.host = host || '127.0.0.1';
  this.port = port || 8125;

  //
  // Optional shared socket
  //
  this.socket = options && options.socket;

  //
  // Handle prefix
  //
  this.scope = options && options.scope || options && options.prefix || '';

  //
  // groups in graphite are delimited by `.` so we need to make sure our
  // scope ends with `.`. If it doesn't we just add it (unless we have no
  // scope defined).
  //
  if(typeof this.scope === 'string' && this.scope !== '' &&
     !/\.$/.test(this.scope)) {
    this.scope += '.';
  }

  //
  // When a *shared* socked isn't provided, an ephemeral
  // socket is demand allocated.  This ephemeral socket is closed
  // after being idle for EPHEMERAL_LIFETIME_MS.
  //
  this.ephemeral_socket = undefined;
  this.last_used_timer  = undefined;

  //
  // Set out error handling code
  //
  this.on_error = options && typeof options.on_error === 'function'
               ? options.on_error
               : this._default_error_handler
               ; 

  //
  // Stream properties
  //
  this.readable = true;
  this.writable = true;

  this.parser = parser.createStream();

  this.parser.on('error', this.on_error);

  this.parser.on('stat', function (text, stat_obj) {
    var stat = {};

    //
    // Construct a statsd value|type pair
    //
    stat[stat_obj.stat] = stat_obj.value + '|' + stat_obj.type;

    //
    // Add sample rate if one exists
    //
    if(stat_obj.sample_rate) {
      stat[stat_obj.stat] += '@' + stat_obj.sample_rate;
      self.send(stat, parseFloat(stat_obj.sample_rate));
    }
    else {
      self.send(stat);
    }
  });
}

util.inherits(Lynx, Stream);

//
// ### constructor Timer(stat, sample_rate)
// #### @stat        {String} Stat key, in `foo:1|ms` would be foo
// #### @sample_rate {Number} Determines the sampling rate, e.g. how many
//      packets should be sent. If set to 0.1 it sends 1 in each 10.
//
// var client = new lynx('localhost', 8125);
// var timer  = client.Timer('foo');
//
// //
// // Sends something like: `foo:100|ms` via udp to the server
// //
// setTimeout(function {
//   timer.stop();
// }, 100);
//
// Returns a timer. When stopped, this transmits an interval
//
Lynx.prototype.createTimer = function createTimer(stat, sample_rate) {
  var self       = this
    , start_time = new Date ().getTime()
    , stopped    = false
    , duration
    , start_hrtime
    ;

  if (typeof process.hrtime === "function") {
    var start_hrtime = process.hrtime();
  }

  //
  // ### function stop()
  //
  // Stops the timer and issues the respective interval.
  // Check example above
  //
  function stop() {
    //
    // If timer is already stopped just ignore the request
    //
    if(stopped) {
      self.on_error(
        makeError({ message : "Can't stop a timer twice"
        , f       : 'stop'
        }));
      return;
    }

    //
    // Calculate duration
    //
    if (start_hrtime) {
      var stop_hrtime = process.hrtime()
        , seconds     = stop_hrtime[0] - start_hrtime[0]
        , nanos       = stop_hrtime[1] - start_hrtime[1]
        ;
      duration = seconds * 1000 + nanos / 1000000
    } else {
      duration = new Date ().getTime() - start_time;
    }

    //
    // Emit
    //
    self.timing(stat, duration, sample_rate);

    //
    // So no one stops a timer twice (causing two emits)
    //
    stopped = true;

    return duration;
  }

  //
  // The closure that is returned
  //
  return {
      stat        : stat
    , sample_rate : sample_rate
    , start_time  : start_time
    , stop        : stop
  };
};

//
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ api ~~
//

//
// ### function increment(stats, sample_rate)
// #### @stats       {String|Array} Stat key, in `foo:1|ms` would be foo
//      Optionally an array of `stats`.
// #### @sample_rate {Number} Determines the sampling rate, e.g. how many
//      packets should be sent. If set to 0.1 it sends 1 in each 10.
//
// var client = new lynx('localhost', 8125);
// client.increment('getho');
// client.increment(['not', 'cool']);
//
// Incremenents the desired stat(s)
//
Lynx.prototype.increment = function increment(stats, sample_rate) {
  this.count(stats, 1, sample_rate);
};

//
// ### function decrement(stats, sample_rate)
// #### @stats       {String|Array} Stat key, in `foo:1|ms` would be foo
//      Optionally an array of `stats`.
// #### @sample_rate {Number} Determines the sampling rate, e.g. how many
//      packets should be sent. If set to 0.1 it sends 1 in each 10.
//
// var client = new lynx('localhost', 8125);
// client.decrement('hey.you');
//
// Decrements the desired stat(s)
//
Lynx.prototype.decrement = function decrement(stats, sample_rate) {
  this.count(stats, -1, sample_rate);
};

//
// ### function count(stats, delta, sample_rate)
// #### @stats       {String|Array} Stat key, in `foo:1|ms` would be foo
//      Optionally an array of `stats`.
// #### @delta       {Number} Amount to add (or remove) from given stat
// #### @sample_rate {Number} Determines the sampling rate, e.g. how many
//      packets should be sent. If set to 0.1 it sends 1 in each 10.
//
// var client = new lynx('localhost', 8125);
// client.count('python.fun', -100);
//
// Sends counting information to statsd. Normally this is invoked via 
// `increment` or `decrement`
//
Lynx.prototype.count = function count(stats, delta, sample_rate) {
  //
  // If we are given a string stat (key) then transform it into array
  //
  if (typeof stats === 'string') {
    stats = [stats];
  }

  //
  // By now stats must be an array
  //
  if(!Array.isArray(stats)) {
    //
    // Error: Can't set if its not even an array by now
    //
    this.on_error(
      makeError({ message : "Can't set if its not even an array by now"
      , f       : 'count'
      , args    : arguments
      }));
    return;
  }

  //
  // Delta is required and must exist or we will send crap to statsd
  //
  if (typeof delta!=='number' && typeof delta!=='string' || isNaN(delta)) {
    //
    // Error: Must be either a number or a string, we cant send other stuff
    //
    this.on_error(
      makeError({ message : 'Must be either a number or a string'
      , f       : 'count'
      , args    : arguments
      }));
    return;
  }

  //
  // Batch up all these stats to send
  //
  var batch = {};
  for(var i in stats) {
    batch[stats[i]] = delta + '|c';
  }

  //
  // Send all these stats
  //
  this.send(batch, sample_rate);
};

//
// ### function timing(stat, duration, sample_rate)
// #### @stat        {String} Stat key, in `foo:1|ms` would be foo
// #### @duration    {Number} Timing duration in ms.
// #### @sample_rate {Number} Determines the sampling rate, e.g. how many
//      packets should be sent. If set to 0.1 it sends 1 in each 10.
//
// var client = new lynx('localhost', 8125);
// client.timing('foo.bar.time', 500);
//
// Sends timing information for a given stat.
//
Lynx.prototype.timing = function timing(stat, duration, sample_rate) {
  var stats   = {};
  stats[stat] = duration + '|ms';
  this.send(stats, sample_rate);
};

//
// ### function set(stat, value, sample_rate)
// #### @stat        {String} Stat key, in `foo:1|s` would be foo
// #### @value       {Number} Value for this set
// #### @sample_rate {Number} Determines the sampling rate, e.g. how many
//      packets should be sent. If set to 0.1 it sends 1 in each 10.
//
// var client = new lynx('localhost', 8125);
// client.set('set1.bar', 567);
//
// Set for a specific stat
//
Lynx.prototype.set = function set(stat, value, sample_rate) {
  var stats   = {};
  stats[stat] = value + '|s';
  this.send(stats, sample_rate);
};

//
// ### function gauge(stat, value, sample_rate)
// #### @stat        {String} Stat key, in `foo:1|g` would be foo
// #### @value       {Number} Value for this set
// #### @sample_rate {Number} Determines the sampling rate, e.g. how many
//      packets should be sent. If set to 0.1 it sends 1 in each 10.
//
// var client = new lynx('localhost', 8125);
// client.gauge('gauge1.bar', 567);
//
// Send a gauge to statsd
//
Lynx.prototype.gauge = function gauge(stat, value, sample_rate) {
  var stats   = {};
  stats[stat] = value + '|g';
  this.send(stats, sample_rate);
};

//
// ### function send(stats, sample_rate)
// #### @stats       {Object} A stats object
// #### @sample_rate {Number} Determines the sampling rate, e.g. how many
//      packets should be sent. If set to 0.1 it sends 1 in each 10.
//
// var lynx = require('lynx');
// var client = new lynx('localhost', 8125);
// client.send(
//   { "foo" : "1|c"
//   , "bar" : "-1|c"
//   , "baz" : "500|ms"
//   });
//
// Will sample this data for a given sample_rate. If a random generated
// number matches that sample_rate then stats get returned and the sample
// rate gets appended ("|@0.5" in this case). Else we get an empty object.
//
Lynx.prototype.send = function send(stats, sample_rate) {
  var self          = this
    , sampled_stats = Lynx.sample(stats, sample_rate)
    , all_stats     = Object.keys(sampled_stats)
    //
    // Data to be sent
    //
    , send_data
    ;

  //
  // If this object is empty (enumerable properties)
  //
  if(all_stats.length === 0) {
    //
    // Error: Nothing to send
    //
    this.on_error(
      makeError({ message : 'Nothing to send'
      , f       : 'send'
      , args    : arguments
      }));
    return;
  }

  //
  // Construct our send request
  // If we have multiple stats send them in the same udp package
  // This is achieved by having newline separated stats.
  //
  send_data = all_stats.map(function construct_stat(stat) {
    return self.scope + stat + ':' + sampled_stats[stat];
  }).join('\n');

  //
  // Encode our data to a buffer
  //
  var buffer = new Buffer(send_data, 'utf8')
    , socket
    ;

  //
  // Do we already have a socket object we can use?
  //
  if (this.socket === undefined) {
    //
    // Do we have an ephemeral socket we can use?
    //
    if (!this.ephemeral_socket) {
      //
      // Create one
      //
      this.ephemeral_socket = dgram.createSocket('udp4');

      //
      // Register on error: Failed sending the buffer
      //
      this.ephemeral_socket.on('error', function (err) {
        err.reason  = err.message;
        err.f       = 'send';
        err.message = 'Failed sending the buffer';
        err.args    = arguments;
        self.on_error(err);
        return;
      });
    }

    socket = this.ephemeral_socket;
  } else {
    //
    // Reuse our socket
    //
    socket = this.socket;
  }

  //
  // Update the last time this socket was used
  // This is used to make the socket ephemeral
  //
  this._update_last_used();

  //
  // Send the data
  //
  this.emit('data', buffer);
  socket.send(buffer, 0, buffer.length, this.port, this.host, noop);
};

//
// ### function close()
//
// var client = new lynx('localhost', 8125);
// client.increment("zigzag");
// client.close();
//
// Closes our socket object after we are done with it
//
Lynx.prototype.close = function close() {
  //
  // User defined socket
  //
  if (this.socket) {
    this.socket.close();
    this.socket = undefined;
  }

  //
  // Ephemeral socket
  //
  if (this.ephemeral_socket) {
    this.ephemeral_socket.close();
    this.ephemeral_socket = undefined;
  }

  //
  // Timer
  //
  if (this.last_used_timer) {
    clearTimeout(this.last_used_timer);
    this.last_used_timer = undefined;
  }
};


//
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ streams ~~
//

//
// ### function write()
//
// Implements `Stream.prototype.write()`.
//
Lynx.prototype.write = function write(buffer) {
  this.parser.write(buffer);
};

//
// ### function end()
//
// Implements `Stream.prototype.end()`.
//
Lynx.prototype.end = function end(buffer) {
  //
  // If there's stuff to flush please do
  //
  if (arguments.length) {
    this.write(buffer);
  }

  //
  // Make this not writable
  //
  this.writable = false;
};

//
// ### function destroy()
//
// Implements `Stream.prototype.destroy()`. Nothing to do here, we don't
// open any stuff
//
Lynx.prototype.destroy = function destroy() {
  this.writable = false;
};


//
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ aux ~~
//

//
// ### function sample(stats, sample_rate)
// #### @stats       {Object} A stats object
// #### @sample_rate {Number} Determines the sampling rate, e.g. how many
//      packets should be sent. If set to 0.1 it sends 1 in each 10.
//
// var lynx = require('lynx');
// lynx.sample(
//   { "foo" : "1|c"
//   , "bar" : "-1|c"
//   , "baz" : "500|ms"
//   }, 0.5);
//
// Will sample this data for a given sample_rate. If a random generated
// number matches that sample_rate then stats get returned and the sample
// rate gets appended ("|@0.5" in this case). Else we get an empty object.
//
Lynx.sample = function sample(stats, sample_rate) {
  //
  // If we don't have a sample rate between 0 and 1
  //
  if (typeof sample_rate !== 'number' || sample_rate > 1 || sample_rate < 0) {
    //
    // Had to ignore the invalid sample rate
    // Most of the times this is because sample_rate is undefined
    //
    return stats;
  }

  var sampled_stats = {};

  //
  // Randomly determine if we should sample this specific instance
  //
  if (mt.genrand_real2(0,1) <= sample_rate) {
    //
    // Note: Current implementation either sends all stats for a specific
    //       sample rate or sends none. Makes one wonder if granularity
    //       should be at the individual stat level
    //
    Object.keys(stats).forEach(function construct_sampled(stat) {
      var value = stats[stat];
      sampled_stats[stat] = value + '|@' + sample_rate;
    });
  }

  return sampled_stats;
};

//
// ### function _update_last_used()
//
// An internal function update the last time the socket was
// used.  This function is called when the socket is used
// and causes demand allocated ephemeral sockets to be closed
// after a period of inactivity.
//
Lynx.prototype._update_last_used = function _update_last_used() {
  var self = this;

  //
  // Only update on the ephemeral socket
  //
  if (this.ephemeral_socket) {
    //
    // Clear existing timeouts
    //
    if (this.last_used_timer) {
      clearTimeout(this.last_used_timer);
    }

    //
    // Update last_used_timer
    //
    this.last_used_timer = setTimeout(function() {
      //
      // If we have an open socket close it
      //
      if (self.ephemeral_socket) {
        self.ephemeral_socket.close();
      }

      //
      // Delete the socket
      //
      delete self.ephemeral_socket;
    }, EPHEMERAL_LIFETIME_MS);
  }
};

//
// ### function default_error_handler()
// #### @err  {Object} The error object. Includes:
// err.message, err.*
//
// Function that defines what to do on error.
// Errors are soft errors, and while interesting they are mostly informative
// A simple console log would do but that doesn't allow people to do
// custom stuff with errors
//
Lynx.prototype._default_error_handler = function _default_error_handler(e) {
  this.emit('error', e);
};

//
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ exports ~~
//

module.exports = Lynx;
