RankedSet = require '../src/ranked_set'
Promise = require 'bluebird'
_ = require 'lodash'
fdb = require('fdb').apiVersion(300)

describe 'The Ranked Set data structure', ->

  beforeEach (next) ->

    @encodeValue = (value) ->
      data = new Buffer(4)
      data.writeUInt32LE(value, 0)
      data

    @decodeValue = (buffer) ->
      buffer.readUInt32LE(0)

    @transaction (tr) =>
      tr.clearRange(@subspace.pack([]), @subspace.pack([0xff]))
    .then ->
      next()
    .done()

    return

  describe 'the create function', ->

    it 'sets up the header for the ranked set', (next) ->
      subspace = @subspace.subspace(['rs'])
      range = subspace.range()
      @transaction (tr) ->
        RankedSet.create(tr, subspace)
      .then =>
        @transaction (tr) =>
          tr.getRange(range.begin, range.end).toArray()
          .then (results) =>
            @assertKVPairsAreEqual(subspace, results, [
              [0,''], @encodeValue(0)
              [1,''], @encodeValue(0)
              [2,''], @encodeValue(0)
              [3,''], @encodeValue(0)
              [4,''], @encodeValue(0)
              [5,''], @encodeValue(0)
            ])
            Promise.resolve()
      .then ->
        next()
      .done()
      return

  describe 'the insert function', ->

    it 'adds the value in the set', (next) ->
      subspace = @subspace.subspace(['rs'])
      range = subspace.range()
      items = []
      @transaction (tr) ->
        RankedSet.create(tr, subspace)
        .then =>
          Promise.reduce [1..100], (total, index) ->
            item = parseInt(Math.random() * 100) * (if Math.random() > 0.5 then 1 else -1)
            items.push item
            RankedSet.insert(tr, subspace, item)
          , 0
      .then =>
        items = _.uniq(items)
        range = subspace.range()
        counts = {}
        @transaction (tr) =>
          tr.getRange(range.begin, range.end).toArray()
          .then (results) =>
            for pair in results
              [level, key] = subspace.unpack(pair.key)
              # Every key must be an inserted item
              expect(items.indexOf(key) >= 0).to.equal.true
              counts[level] ?= 0
              counts[level] += @decodeValue(pair.value)
        .then =>
          # The total count on each level must equal the total number of items
          for level, total of counts
            total.should.equal items.length
      .then ->
        next()
      .done()
      return

    it 'throws an error if multiple inserts occur simultaneously in a single transaction', (next) ->
      subspace = @subspace.subspace(['rs'])
      range = subspace.range()
      @transaction (tr) ->
        RankedSet.create(tr, subspace)
        .then =>
          queue = []
          for index in [1..100]
            queue.push RankedSet.insert(tr, subspace, index)
          Promise.all(queue)
      .catch (err) ->
        err.message.should.equal("simultaneous writes within single transaction detected on ranked set subspace test:rs")
        next()
      .done()
      return

  describe 'the contains function', ->

    it 'returns true if the key is present in the set and false otherwise', (next) ->
      subspace = @subspace.subspace(['rs'])
      items = []
      @transaction (tr) ->
        RankedSet.create(tr, subspace)
        .then =>
          Promise.reduce [1..100], (total, index) ->
            item = parseInt(Math.random() * 100) * (if Math.random() > 0.5 then 1 else -1)
            items.push item
            RankedSet.insert(tr, subspace, item)
          , 0
      .then =>
        items = _.uniq(items)
        @transaction (tr) ->
          Promise.reduce [1..100], (total, index) ->
            present_item = items[parseInt(Math.random() * items.length)]
            absent_item = parseInt(Math.random() * 200 + 150)
            Promise.all([
              RankedSet.contains(tr, subspace, present_item)
              RankedSet.contains(tr, subspace, absent_item)
            ]).spread (result1, result2) ->
              result1.should.be.true
              result2.should.be.false
          , 0
        .then ->
          next()
      .done()
      return

  describe 'the remove function', ->

    beforeEach (next) ->
      subspace = @subspace.subspace(['rs'])
      @items = []
      @transaction (tr) =>
        RankedSet.create(tr, subspace)
        .then =>
          Promise.reduce [1..100], (total, index) =>
            item = parseInt(Math.random() * 100) * (if Math.random() > 0.5 then 1 else -1)
            @items.push item
            RankedSet.insert(tr, subspace, item)
          , 0
      .then ->
        next()
      .done()
      return

    it 'removes the key from the set', (next) ->
      subspace = @subspace.subspace(['rs'])
      range = subspace.range()
      items = _.uniq @items
      removed_items = []
      @transaction (tr) ->
        Promise.reduce [1..50], (total, index) ->
          item = items[parseInt(Math.random() * items.length)]
          removed_items.push item
          RankedSet.remove(tr, subspace, item)
        , 0
      .then =>
        @transaction (tr) ->
          # Contains should return false for all items
          Promise.reduce removed_items, (total, removed_item) ->
            RankedSet.contains(tr, subspace, removed_item)
            .then (is_present) ->
              is_present.should.equal false
      .then =>
        @transaction (tr) ->
          # Ensure none of the items are remaining in the set
          tr.getRange(range.begin, range.end).toArray()
          .then (results) =>
            for pair in results
              [level, key] = subspace.unpack(pair.key)
              if key is '' then continue
              # Every key must not be present in the items or
              if removed_items.indexOf(key) >= 0
                throw Error("Removed key #{key} found in ranked set")
              else
                expect(items.indexOf(key) >= 0).to.be.true
                expect(removed_items.indexOf(key) >= 0).to.be.false
            Promise.resolve()
      .then ->
        next()
      .done()
      return

    it 'throws an error if multiple inserts occur simultaneously in a single transaction', (next) ->
      subspace = @subspace.subspace(['rs'])
      range = subspace.range()
      @transaction (tr) ->
        RankedSet.create(tr, subspace)
        .then =>
          queue = []
          for index in [1..100]
            queue.push RankedSet.remove(tr, subspace, index)
          Promise.all(queue)
      .catch (err) ->
        err.message.should.equal("simultaneous writes within single transaction detected on ranked set subspace test:rs")
        next()
      .done()
      return

  describe 'the clear function', ->

    beforeEach (next) ->
      subspace = @subspace.subspace(['rs'])
      @transaction (tr) =>
        RankedSet.create(tr, subspace)
        .then =>
          Promise.reduce [1..100], (total, index) =>
            item = parseInt(Math.random() * 100)
            RankedSet.insert(tr, subspace, item)
          , 0
      .then ->
        next()
      .done()
      return

    it 'wipes out all data in the ranked set', (next) ->
      subspace = @subspace.subspace(['rs'])
      range = subspace.range()
      @transaction (tr) =>
        RankedSet.clear(tr, subspace).then =>
          tr.getRange(range.begin, range.end).toArray()
        .then (results) =>
          @assertKVPairsAreEqual(subspace, results, [
            [0,''], @encodeValue(0)
            [1,''], @encodeValue(0)
            [2,''], @encodeValue(0)
            [3,''], @encodeValue(0)
            [4,''], @encodeValue(0)
            [5,''], @encodeValue(0)
          ])
          Promise.resolve()
      .then ->
        next()
      .done()
      return

  describe 'the rank function', ->

    beforeEach (next) ->
      subspace = @subspace.subspace(['rs'])
      @items = []
      @transaction (tr) =>
        RankedSet.create(tr, subspace)
        .then =>
          Promise.reduce [1..100], (total, index) =>
            item = parseInt(Math.random() * 100)
            @items.push item
            RankedSet.insert(tr, subspace, item)
          , 0
      .then ->
        next()
      .done()
      return

    it 'returns the rank of an item in the set', (next) ->
      items = _.uniq @items
      items.sort (a, b) -> a - b
      subspace = @subspace.subspace(['rs'])
      @transaction (tr) =>
        Promise.reduce items, (total, item) =>
          RankedSet.rank(tr, subspace, item)
          .then (rank) ->
            rank.should.equal items.indexOf(item)
            Promise.resolve()
        , 0
      .then ->
        next()
      .done()
      return

  describe 'the getNth function', ->

    beforeEach (next) ->
      subspace = @subspace.subspace(['rs'])
      @items = []
      @transaction (tr) =>
        RankedSet.create(tr, subspace)
        .then =>
          Promise.reduce [1..100], (total, index) =>
            item = parseInt(Math.random() * 100)
            @items.push item
            RankedSet.insert(tr, subspace, item)
          , 0
      .then ->
        next()
      .done()
      return

    it 'returns the item with rank N from the set', (next) ->
      items = _.uniq @items
      items.sort (a, b) -> a - b
      subspace = @subspace.subspace(['rs'])
      @transaction (tr) =>
        Promise.reduce items, (total, item) =>
          RankedSet.getNth(tr, subspace, items.indexOf(item))
          .then (set_item) ->
            set_item.should.equal item
            Promise.resolve()
        , 0
      .then ->
        next()
      .done()
      return


  describe 'the getRange function', ->

    beforeEach (next) ->
      subspace = @subspace.subspace(['rs'])
      @items = []
      @transaction (tr) =>
        RankedSet.create(tr, subspace)
        .then =>
          Promise.reduce [1..100], (total, index) =>
            item = parseInt(Math.random() * 100)
            @items.push item
            RankedSet.insert(tr, subspace, item)
          , 0
      .then ->
        next()
      .done()
      return

    it 'returns a range of items from the set', (next) ->
      @items = _.uniq @items
      @items.sort (a,b) -> a - b
      subspace = @subspace.subspace(['rs'])
      @transaction (tr) =>
        start_index = parseInt(Math.random() * @items.length)
        end_index = start_index + parseInt(Math.random() * (@items.length - start_index))
        RankedSet.getRange(tr, subspace, @items[start_index], @items[end_index])
        .then (results) =>
          results.should.deep.equal @items.slice(start_index, end_index)
          next()
      .done()
      return


