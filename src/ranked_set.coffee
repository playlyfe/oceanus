_ = require 'lodash'
Promise = require 'bluebird'
fdb = require('fdb').apiVersion(300)
xxhash = require 'xxhashjs'

# Constants
MAX_LEVELS = 6
LEVEL_FAN_POW = 4
levels = [0..MAX_LEVELS - 1]
fan_limits = _.map(levels, (level) -> Math.pow(2, level * LEVEL_FAN_POW) - 1)

# Private Methods

getLock = (tr, subspace) ->
  locks = tr.locks ?= {}
  lock_key = fdb.tuple.unpack(subspace.key()).join(':')
  if locks[lock_key]
    Promise.reject(new Error("simultaneous writes within single transaction detected on ranked set subspace #{lock_key}"))
  else
    locks[lock_key] = true
    Promise.resolve().disposer () ->
      delete locks[lock_key]

encodeCount = (c) ->
  value = new Buffer(4)
  value.writeInt32LE(c, 0)
  value

decodeCount = (v) ->
  v.readInt32LE(0)

get_previous_node = (tr, subspace, level, key) ->
  k = subspace.pack([level, key])
  tr.snapshot.getRange(fdb.KeySelector.lastLessThan(k), fdb.KeySelector.firstGreaterOrEqual(k), { limit: 1}).toArray()
  .then ([kv]) ->
    prev_key = subspace.unpack(kv.key)[1]
    tr.addReadConflictRange(kv.key, k)
    Promise.resolve(prev_key)

slow_count = (tr, subspace, level, begin_key, end_key) ->
  if level is -1
    if begin_key is ''
      return Promise.resolve(0)
    else
      return Promise.resolve(1)
  sum = 0
  tr.getRange(subspace.pack([level, begin_key]), subspace.pack([level, end_key])).forEach((pair, callback) ->
    sum += decodeCount(pair.value)
    callback()
  ).then ->
    Promise.resolve(sum)

_rank = (tr, subspace, key, level, result) ->
  level_ss = subspace.subspace([level])
  last_count = 0
  _defer = Promise.defer()
  tr.getRange(level_ss.pack([result.rank_key]), fdb.KeySelector.firstGreaterThan(level_ss.pack([key]))).forEach((pair, callback) ->
    result.rank_key = level_ss.unpack(pair.key)[0]
    last_count = decodeCount(pair.value)
    result.r += last_count
    callback()
  , (err) ->
    if err then _defer.reject(err)
    else
      result.r -= last_count
      if result.rank_key is key or level < 0
        _defer.resolve()
      else
        _defer.resolve(_rank(tr, subspace, key, level - 1, result))
  )
  _defer.promise

_getNth = (tr, subspace, level, result) ->
  level_ss = subspace.subspace([level])
  tr.getRange(level_ss.pack([result.key]), level_ss.range().end).toArray()
  .then (pairs) ->

    for pair in pairs
      result.key = level_ss.unpack(pair.key)[0]
      count = decodeCount(pair.value)
      if result.key isnt '' and result.key isnt null and result.r is 0
        return Promise.resolve()

      if count > result.r
        break

      result.r -= count

    if pairs.length is 0 or level is 0
      Promise.resolve(null)
    else
      Promise.resolve(_getNth(tr, subspace, level - 1, result))

# Public Methods

create = (tr, subspace) ->
  # Setup levels
  queue = []
  _.forEach levels, (level) ->
    level_ss = subspace.pack([level, ''])
    queue.push tr.get(level_ss).then (result) ->
      if not result?
        tr.set(level_ss, encodeCount(0))
      Promise.resolve()
  Promise.all(queue)

rank = (tr, subspace, key) ->
  if key is '' or key is null
    throw new Error('Empty key not allowed in set')

  contains(tr, subspace, key)
  .then (is_present) ->
    if not is_present
      Promise.resolve(null)
    else
      result = { r: 0, rank_key: '' }
      _rank(tr, subspace, key, MAX_LEVELS, result).then ->
        Promise.resolve(result.r)

getNth = (tr, subspace, rank) ->

  if rank < 0
    Promise.resolve(null)

  result = { r: rank, key: '' }

  _getNth(tr, subspace, MAX_LEVELS - 1, result)
  .then ->
    Promise.resolve(result.key)

getRange = (tr, subspace, start_key, end_key) ->

  if start_key is '' or start_key is null
    throw new Error('Empty key not allowed in set')

  tr.getRange(subspace.pack([0, start_key]), subspace.pack([0, end_key])).toArray()
  .then (result) ->
    items = []
    for pair in result
      items.push subspace.unpack(pair.key)[1]
    Promise.resolve(items)

contains = (tr, subspace, key) ->
  if key is '' or key is null
    throw new Error('Empty key not allowed in set')
  tr.get(subspace.pack([0, key])).then (result) ->
    Promise.resolve(result isnt null)

insert = (tr, subspace, key) ->

  contains(tr, subspace, key)
  .then (is_present) ->
    if is_present
      Promise.resolve(key)
    else
      Promise.using(getLock(tr, subspace), () ->
        # We assume the key is an integer
        hash = Math.abs(xxhash(key.toString(), 11235813).toNumber())
        Promise.reduce(levels, (total, level) ->
          get_previous_node(tr, subspace, level, key).then (prev_key) ->
            if hash & fan_limits[level]
              Promise.resolve(tr.add(subspace.pack([level, prev_key]), encodeCount(1)))
            else
              Promise.all([
                tr.get(subspace.pack([level, prev_key]))
                slow_count(tr, subspace, level - 1, prev_key, key)
              ]).spread (_prev_count, _count) ->
                prev_count = decodeCount(_prev_count)
                new_prev_count = _count
                count = prev_count - new_prev_count + 1
                tr.set(subspace.pack([level, prev_key]), encodeCount(new_prev_count))
                tr.set(subspace.pack([level, key]), encodeCount(count))
                Promise.resolve()
        , 0)
      )

remove = (tr, subspace, key) ->

  contains(tr, subspace, key)
  .then (is_present) ->
    if not is_present
      Promise.resolve()
    else
      Promise.using(getLock(tr, subspace), () ->
        Promise.reduce(levels, (total, level) ->
          k = subspace.pack([level, key])
          tr.get(k).then (c) ->
            if c isnt null
              tr.clear(k)
            if level is 0
              return Promise.resolve()
            get_previous_node(tr, subspace, level, key).then (prev_key) ->
              if prev_key is key
                throw Error("key #{key} is same as previous key #{prev_key}")
              count_change = -1
              if c isnt null
                count_change += decodeCount(c)
              tr.add(subspace.pack([level, prev_key]), encodeCount(count_change))
        , 0)
      )

clear = (tr, subspace, key) ->
  range = subspace.range()
  tr.clearRange(range.begin, range.end)
  create(tr, subspace)

print = (tr, subspace) ->
  range = subspace.range([])
  range_total = 0
  current_range = 0
  tr.getRange(range.begin, range.end).forEach((item, callback) ->
    path = subspace.unpack(item.key)
    console.log "#{path} = #{decodeCount(item.value)}"
    if path[0] is current_range
      range_total += decodeCount(item.value)
    else
      range_total = decodeCount(item.value)
      current_range = path[0]
    callback()
  )

module.exports = {

  create: create

  insert: insert

  contains: contains

  remove: remove

  clear: clear

  rank: rank

  getNth: getNth

  getRange: getRange

  print: print

}

