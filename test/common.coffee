Promise = require 'bluebird'
global.chai = require 'chai'
global.should = chai.should()
global.expect = chai.expect

before (next) ->

  @fdb = require('fdb').apiVersion(300)
  @db = @fdb.open('/home/playlyfe/config/fdb.cluster')
  @subspace = new @fdb.Subspace(['test'])

  @assertKVPairsAreEqual = (subspace, actual_pairs, expected_pairs) =>
    for index, pair of actual_pairs
      if not expected_pairs[index * 2]? or not expected_pairs[index * 2 + 1]?
        throw new Error("No more pairs, index: #{index}", 0)
      expected_pairs[index * 2].should.deep.equal(subspace.unpack(pair.key))
      expected_pairs[index * 2 + 1].should.deep.equal(pair.value)
    return

  @transaction = (fn) =>
    Promise.resolve(@db.doTransaction((tr, callback) ->
      Promise.resolve(fn(tr)).nodeify(callback)
    ))

  @clearAllKeys = () =>
    @transaction (tr) =>
      tr.clearRange(@subspace.pack([]), @subspace.pack([0xff]))

  next()
