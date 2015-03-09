OceanusUtil = require '../src/util'
Promise = require 'bluebird'
fdb = require('fdb').apiVersion(300)

describe 'The util functions', ->

  describe 'The registerPath function', ->

    it 'registers a function for opening a path', ->


  describe 'The open function', ->

    before ->

      partition_layer = new Buffer('partition')

      OceanusUtil.registerPath 'app_dir', (tr, params, cache, path) ->
        cache_key = "pl_core"
        if cache[cache_key]?
          cache[cache_key]
        else
          cache[cache_key] = Promise.resolve(fdb.directory.createOrOpen(tr, ['pl_core']))

      OceanusUtil.registerPath 'game_dir', (tr, params, cache) ->
        {game_id} = params
        cache_key = "pl_core:g:#{game_id}"
        if cache[cache_key]?
          cache[cache_key]
        else
          @open(tr, 'app_dir', params)
          .then (app_dir) ->
            cache[cache_key] = Promise.resolve(app_dir.createOrOpen(tr, ['games', game_id], { layer: partition_layer }))

      OceanusUtil.registerPath 'runtime_dir', (tr, params, cache) ->
        {game_id, runtime_id} = params
        cache_key = "pl_core:g:#{game_id}:r:#{runtime_id}"
        if cache[cache_key]?
          cache[cache_key]
        else
          @open(tr, 'game_dir', params)
          .then (game_dir) ->
            cache[cache_key] = Promise.resolve(game_dir.createOrOpen(tr, ['runtime', runtime_id]))

      OceanusUtil.registerPath 'leaderboard_instance_dir', (tr, params, cache) ->
        {game_id, runtime_id} = params
        cache_key = "pl_core:g:#{game_id}:r:#{runtime_id}:lbi"
        if cache[cache_key]?
          cache[cache_key]
        else
          @open(tr, 'runtime_dir', params)
          .then (runtime_dir) ->
            cache[cache_key] = Promise.resolve(runtime_dir.createOrOpen(tr, ['leaderboards'], { layer: partition_layer } ))

      OceanusUtil.registerPath 'leaderboard_instance', (tr, params, cache) ->
        {game_id, runtime_id, leaderboard_instance_id} = params
        cache_key = "pl_core:g:#{game_id}:r:#{runtime_id}:lbi:#{leaderboard_instance_id}"
        if cache[cache_key]?
          cache[cache_key]
        else
          @open(tr, 'leaderboard_instance_dir', params)
          .then (leaderboard_instance_dir) ->
            cache[cache_key] = Promise.resolve(leaderboard_instance_dir.createOrOpen(tr, [leaderboard_instance_id]))

    it 'opens a path that was previously registered', (next) ->
      now = new Date()
      Promise.map([1...1000], (index) =>
        console.log "start #{index}"
        @transaction (tr) ->
          OceanusUtil.open(tr, 'leaderboard_instance', { game_id: 'test', runtime_id: 'staging', leaderboard_instance_id: "foo/bar:2015-01-01:m:r:y:#{Math.random() * 10000000}" })
          .then (dir) ->
            console.log fdb.tuple.unpack(dir.key())
        .then =>
          console.log "stop #{index}"
      )
      .then ->
        console.log "Done in #{new Date() - now}ms"
        next()
      .done()
      return

