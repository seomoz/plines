require 'redis'
require 'redis/value'
require 'redis/lock'
require 'redis/set'
require 'redis/list'
require 'redis/hash_key'
require 'redis/counter'

module Plines
  module RedisObjectsHelpers
    def new_redis_object(klass, key, args)
      klass.new([key_prefix, key].join(':'), redis, *args)
    end

    def key_prefix
      @key_prefix ||= [
        self.class.module_key_prefix,
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
      def value(name, *args)
        declared_redis_object_names << name

        class_eval <<-EOS, __FILE__, __LINE__ + 1
          def #{name}
            @#{name} ||= new_redis_object(::Redis::Value, #{name.inspect}, #{args})
          end
        EOS
      end

      def lock(name, *args)
        declared_redis_object_names << name

        class_eval <<-EOS, __FILE__, __LINE__ + 1
          def #{name}
            @#{name} ||= new_redis_object(::Redis::Lock, #{name.inspect}, #{args})
          end
        EOS
      end

      def set(name, *args)
        declared_redis_object_names << name
        class_eval <<-EOS, __FILE__, __LINE__ + 1
          def #{name}
            @#{name} ||= new_redis_object(::Redis::Set, #{name.inspect}, #{args})
          end
        EOS
      end

      def list(name, *args)
        declared_redis_object_names << name

        class_eval <<-EOS, __FILE__, __LINE__ + 1
          def #{name}
            @#{name} ||= new_redis_object(::Redis::List, #{name.inspect}, #{args})
          end
        EOS
      end

      def hash_key(name, *args)
        declared_redis_object_names << name

        class_eval <<-EOS, __FILE__, __LINE__ + 1
          def #{name}
            @#{name} ||= new_redis_object(::Redis::HashKey, #{name.inspect}, #{args})
          end
        EOS
      end

      def counter(name, *args)
        declared_redis_object_names << name

        class_eval <<-EOS, __FILE__, __LINE__ + 1
          def #{name}
            @#{name} ||= new_redis_object(::Redis::Counter, #{name.inspect}, #{args})
          end
        EOS
      end

      def declared_redis_object_names
        @declared_redis_object_names ||= []
      end

      def module_key_prefix(module_key_prefix = 'plines')
        @module_key_prefix ||= module_key_prefix
      end
    end
  end
end

