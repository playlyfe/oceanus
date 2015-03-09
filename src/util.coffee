directory_cache = {}

registered_paths = {}

module.exports = {

  registerPath: (path, open_fn) ->
    if registered_paths[path]?
      throw new Error("Path #{path} has already been registered")
    else
      registered_paths[path] = open_fn

  open: (tr, path, params) ->
    if registered_paths[path]?
      registered_paths[path].call(@, tr, params, directory_cache, path)
    else
      throw new Error("Path #{path} not found")

}
