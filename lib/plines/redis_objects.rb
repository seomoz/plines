require 'redis'
require 'redis/set'
require 'redis/hash_key'
require 'redis/counter'

module Plines
  module RedisObjectsHelpers
    def new_redis_object(klass, key)
      klass.new([key_prefix, key].join(':'), redis)
    end

    def key_prefix
      @key_prefix ||= [
        "plines",
        pipeline.name,
        self.class.name.split('::').last,
        id
      ].join(':')
    end

    def declared_redis_object_keys
      self.class.declared_redis_object_names.map { |n| send(n).key }
    end

    def self.included(klass)
      klass.extend ClassMethods
    end

    module ClassMethods
      def set(name)
        declared_redis_object_names << name

        class_eval <<-EOS, __FILE__, __LINE__ + 1
          def #{name}
            @#{name} ||= new_redis_object(::Redis::Set, #{name.inspect})
          end
        EOS
      end

      def hash_key(name)
        declared_redis_object_names << name

        class_eval <<-EOS, __FILE__, __LINE__ + 1
          def #{name}
            @#{name} ||= new_redis_object(::Redis::HashKey, #{name.inspect})
          end
        EOS
      end

      def counter(name)
        declared_redis_object_names << name

        class_eval <<-EOS, __FILE__, __LINE__ + 1
          def #{name}
            @#{name} ||= new_redis_object(::Redis::Counter, #{name.inspect})
          end
        EOS
      end

      def declared_redis_object_names
        @declared_redis_object_names ||= []
      end
    end
  end
end

