var assert = require("assert");
var microtime = require("microtime-nodejs");

/**
 * determines & returns the type of redis client we've been given
 *
 * @param  {Object}       redis      - redis instance given in options, if any
 * @param  {String}       redis_type - optional, defaults to 'node-redis'
 * @return {String|null}             - NULL if no instance given, else one of: 'node-redis', 'io-redis'
 * @throws {Error}                   - thrown if invalid type is passed in, or given instance does not quack the way it should
 */
function checkRedisInstanceType(redis, redis_type)
{
  var rtype = redis_type || '', valid_types = ['node-redis', 'io-redis'];

  if (!redis)
    return null;

  if (typeof rtype !== 'string')
    rtype = 'node-redis';

  switch (rtype.toLowerCase()) {
    case 'ioredis':
    case 'io_redis':
      rtype = 'io-redis';
      break;
    case '':
    case 'noderedis':
    case 'node_redis':
      rtype = 'node-redis';
      break;
  }

  assert(valid_types.indexOf(rtype) !== -1, '`options.redis_type` must be one of: ' + valid_types.join(', '));

  switch (rtype) {
      // TODO: add duck-typing checks to try to validate the redis instances
      case 'node-redis':
          break;
      case 'io-redis':
          break;
  }

  return rtype;
}

/**
 * determine whether the redis client is/will return responses as [raw] Buffers
 *
 * @param  {Object} redis      [description]
 * @param  {String} redis_type  - required, assumed to be a value returned by `checkRedisInstanceType()`
 * @return {Boolean}
 */
function checkRedisUsesBuffers(redis, redis_type)
{
  if (redis_type === 'node-redis')
    return redis.options.return_buffers || redis.options.detect_buffers;

  return false;
}

function RateLimiter (options) {
  var redis           = options.redis,
      redis_type      = checkRedisInstanceType(redis, options.redis_type),
      interval        = options.interval * 1000, // in microseconds
      maxInInterval   = options.maxInInterval,
      minDifference   = options.minDifference ? 1000 * options.minDifference : null, // also in microseconds
      namespace       = options.namespace || (redis_type && ("rate-limiter-" + Math.random().toString(36).slice(2))) || null;

  var storage, timeouts;

  assert(interval > 0, "Must pass a positive integer for `options.interval`");
  assert(maxInInterval > 0, "Must pass a positive integer for `options.maxInInterval`");
  assert(!(minDifference < 0), "`options.minDifference` cannot be negative");

  if (redis_type) {
    // If redis is going to be potentially returning buffers OR an array from
    // ZRANGE, need a way to safely convert either of these types to an array
    // of numbers.  Otherwise, we can just assume that the result is an array
    // and safely map over it.
    var zrangeToUserSet;
    if (checkRedisUsesBuffers(redis, redis_type)) {
      zrangeToUserSet = function(str) {
        return String(str).split(",").map(Number);
      };
    } else {
      zrangeToUserSet = function(arr) {
        //return Array.isArray(arr) ? arr.map(Number) : Number(arr);
        return arr.map(Number);
      };
    }

    return function (id, cb) {
      if (!cb) {
        cb = id;
        id = "";
      }


      assert.equal(typeof cb, "function", "Callback must be a function.");

      var now = microtime.now();
      var key = namespace + id;
      var clearBefore = now - interval;

      var batch = redis.multi();
      batch.zremrangebyscore(key, 0, clearBefore);
      batch.zrange(key, 0, -1);
      batch.zadd(key, now, now);
      batch.expire(key, Math.ceil(interval / 1000000)); // convert to seconds, as used by redis ttl.
      batch.exec(function (err, resultArr) {
        if (err) return cb(err);

        // response formats:
        // io-redis   : [null, <response>]
        // node_redis : [<response>]
        var i, l, errs;
        if (redis_type === 'io-redis') {
          for (errs = [], i = 0, l = resultArr.length; i < l; i += 1) {
            if (resultArr[i][0])
              errs.push(resultArr[i][0]);
            resultArr[i] = resultArr[i][1];
          }

          if (errs.length) {
            err = new Error('RateLimiter recieved one or more errors from Redis');
            err.errors = errs;

            return cb(err);
          }
        }

        var userSet = zrangeToUserSet(resultArr[1]);

        var tooManyInInterval = userSet.length >= maxInInterval;
        var timeSinceLastRequest = minDifference && (now - userSet[userSet.length - 1]);

        var result, remaining;
        if (tooManyInInterval || timeSinceLastRequest < minDifference) {
          result = Math.min(userSet[0] - now + interval, minDifference ? minDifference - timeSinceLastRequest : Infinity);
          result = Math.floor(result / 1000); // convert to miliseconds for user readability.
          remaining = -1;
        } else {
          remaining = maxInInterval - userSet.length - 1;
          result = 0;
        }

        return cb(null, result, remaining);
      });
    };
  } else {
    storage  = {};
    timeouts = {};

    return function () {
      var args = Array.prototype.slice.call(arguments);
      var cb = args.pop();
      var id;
      if (typeof cb === "function") {
        id = args[0] || "";
      } else {
        id = cb || "";
        cb = null;
      }

      var now = microtime.now();
      var clearBefore = now - interval;

      clearTimeout(timeouts[id]);
      var userSet = storage[id] = (storage[id] || []).filter(function(timestamp) {
        return timestamp > clearBefore;
      });

      var tooManyInInterval = userSet.length >= maxInInterval;
      var timeSinceLastRequest = minDifference && (now - userSet[userSet.length - 1]);

      var result, remaining;
      if (tooManyInInterval || timeSinceLastRequest < minDifference) {
        result = Math.min(userSet[0] - now + interval, minDifference ? minDifference - timeSinceLastRequest : Infinity);
        result = Math.floor(result / 1000); // convert from microseconds for user readability.
        remaining = -1;
      } else {
        remaining = maxInInterval - userSet.length - 1;
        result = 0;
      }
      userSet.push(now);
      timeouts[id] = setTimeout(function() {
        delete storage[id];
      }, interval / 1000); // convert to miliseconds for javascript timeout

      if (cb) {
        return process.nextTick(function() {
          cb(null, result, remaining);
        });
      } else {
        return result;
      }
    };
  }
}

module.exports = RateLimiter;
