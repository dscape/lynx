# lynx

![NPM Downloads](http://img.shields.io/npm/dm/lynx.svg?style=flat) ![NPM Version](http://img.shields.io/npm/v/lynx.svg?style=flat)

A minimalistic node.js client for [statsd] server. Fork of original work by [sivy]

`lynx` features:

* **Minimalistic** — there is only a minimum of abstraction between you and 
  statsd
* **Streams** — You can stream in and out of a `lynx` connection
* **Re-usable UDP Connections** — Keeps UDP connections open for a certain time
* **Errors** — Pluggable error handling, by default errors are ignored

## Quick Start

```
$ npm install lynx
$ node
> var lynx = require('lynx');
//
// Options in this instantiation include:
//   * `on_error` function to be executed when we have errors
//   * `socket` if you wish to just use a existing udp socket
//   * `scope` to define the a prefix for all stats, e.g. with `scope`
//     'product1' and stat 'somestat' the key would actually be
//     'product1.somestat'
//
> var metrics = new lynx('localhost', 8125);
{ host: 'localhost', port: 8125 }
> metrics.increment('node_test.int');
> metrics.decrement('node_test.int');
> metrics.timing('node_test.some_service.task.time', 500); // time in ms
> metrics.gauge('gauge.one', 100);
> metrics.set('set.one', 10);
```

This is the equivalent to:

``` sh
echo "node_test.int:1|c"  | nc -w 0 -u localhost 8125
echo "node_test.int:-1|c" | nc -w 0 -u localhost 8125
echo "node_test.some_service.task.time:500|ms" | nc -w 0 -u localhost 8125
echo "gauge.one:100|g"    | nc -w 0 -u localhost 8125
echo "set.one:10|s"       | nc -w 0 -u localhost 8125
```

The protocol is super simple, so feel free to check out the source code to understand how everything works.

## Advanced

### Sampling

If you want to track something that happens really, really frequently, it can overwhelm StatsD with UDP packets.  To work around that, use the optional sampling rate for metrics.  This will only send packets a certain percentage of time.  For very frequent events, this will give you a statistically accurate representation of your data.

Sample rate is an optional parameter to all of the metric API calls.  A valid sample rate is 0.0 - 1.0.  Values of 0.0 will never send any packets, and values of 1.0 will send every packet.  

In these examples we are samping at a rate of 0.1, meaning 1-in-10 calls to send a sample will actually be sent to StatsD.

```
var metrics = new lynx('localhost', 8125);
metrics.increment('node_test.int', 0.1);
metrics.decrement('node_test.int', 0.1);
metrics.timing('node_test.some_service.task.time', 500, 0.1);
metrics.gauge('gauge.one', 100, 0.1);
metrics.set('set.one', 10, 0.1);
var timer2 = metrics.createTimer('node_test.some_service.task2.time', 0.1);
timer2.stop();
```

### Streams

You can stream to `lynx`:

``` js
fs.createReadStream('file.statsd')
  .pipe(new lynx('localhost', port))
  .pipe(fs.createReadStream('file-fixed.statsd'))
  ;
```

Feel free to check the `stream-test` for more info.

### Timers

If you wish to measure timing you can use the `timer()` functionality.

``` js
var metrics = new lynx('localhost', 8125)
  , timer   = metrics.createTimer('some.interval')
  ;

//
// Should send something like "some.interval:100|ms"
//
setTimeout(function () {
  timer.stop();
}, 100);
```

Timers use `Date.getTime()` which is known for being imprecise at the ms level. If this is a problem to you please submit a pull request and I'll take it.

### Batching

Batching is possible for `increment`, `decrement`, and count:

``` js
metrics.decrement(['uno', 'two', 'trezentos']);
```

If you want to mix more than one type of metrics in a single packet you can use `send`, however you need to construct the values yourself. An example:

``` js
//
// This code is only to exemplify the functionality
//
// As of the current implementation the sample rate is processed per group
// of stats and not per individual stat, meaning either all would be send
// or none would be sent.
//
metrics.send(
  { "foo" : "-1|c"    // count
  , "bar" : "15|g"    // gauge
  , "baz" : "500|ms"  // timing
  , "boaz": "40|s"    // set
  }, 0.1);            // sample rate at `0.1`
```

### Closing your socket

You can close your open socket when you no longer need it by using `metrics.close()`.

### Errors

By default `errors` get emitted. If you wish to change this behavior simply specify a `on_error` function when instantiating the `lynx` client.

``` js
function on_error(err) {
  console.log(err.message);
}

var connection = new lynx('localhost', 1234, {on_error: on_error});
```

Source code is super minimal, if you want try to get familiar with when errors occur check it out. If you would like to change behavior on how this is handled send a pull request justifying why and including the alterations you would like to propose.

## Tests

Run the tests with `npm`.

``` sh
npm test
```

## Meta

           `\.      ,/'
            |\\____//|
            )/_ `' _\(
           ,'/-`__'-\`\
           /. (_><_) ,\
           ` )/`--'\(`'  atc
             `      '

* travis: [![build status](https://secure.travis-ci.org/dscape/lynx.png)](http://travis-ci.org/dscape/lynx)
* code: `git clone git://github.com/dscape/lynx.git`
* home: <http://github.com/dscape/lynx>
* bugs: <http://github.com/dscape/lynx/issues>

`(oo)--',-` in [caos]

[caos]: http://caos.di.uminho.pt
[sivy]: https://github.com/sivy/node-statsd
[statsd]: https://github.com/etsy/statsd
