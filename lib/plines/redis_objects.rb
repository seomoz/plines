require 'redis/objects'

module Plines
  module RedisObjectsHelpers
    def declared_redis_object_keys
      self.class.redis_objects.keys.map { |k| send(k).key }
    end
  end
end

