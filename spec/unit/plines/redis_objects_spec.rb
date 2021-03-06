require 'plines/pipeline'
require 'plines/configuration'

require 'plines/redis_objects'

module Plines
  RSpec.describe RedisObjectsHelpers, :redis do
    class DefaultKeyPrefix < Struct.new(:pipeline, :id)
      include Plines::RedisObjectsHelpers
      value :a_value
      lock :a_lock, expiration: 900, timeout: 60
      list :a_list, marshal: true
      attr_reader :redis

      def initialize(redis, pipeline, id)
        super(pipeline, id)
        @redis = redis
        a_value.set(99) # have to set a value to creat the key
      end
    end

    class OverrideKeyPrefix < DefaultKeyPrefix
      redis_key_prefix 'override'
    end

    it 'passes additional args to the base redis::object' do
      obj = DefaultKeyPrefix.new(redis, pipeline_module, '1234')

      lock_options = obj.a_lock.options
      expect(lock_options[:expiration]).to be(900)
      expect(lock_options[:timeout]).to be(60)

      list_opions = obj.a_list.options
      expect(list_opions[:marshal]).to be true
    end

    it 'contains the pipeline name, class name, and id in the key prefix' do
      DefaultKeyPrefix.new(redis, pipeline_module, '1234')

      redis_keys = redis.keys
      expect(redis_keys.size).to eq(1)

      key_prefix_parts = redis_keys.first.split(':')
      expect(key_prefix_parts).to include('P', 'DefaultKeyPrefix', '1234')
    end

    it 'uses `plines` as the first part of the key prefix by default' do
      DefaultKeyPrefix.new(redis, pipeline_module, '1234')

      redis_keys = redis.keys
      expect(redis_keys.size).to eq(1)

      key_prefix = redis_keys.first.split(':').first
      expect(key_prefix).to eq('plines')
    end

    it 'can override the first part of the key prefix' do
      OverrideKeyPrefix.new(redis, pipeline_module, '1234')

      redis_keys = redis.keys
      expect(redis_keys.size).to eq(1)

      key_prefix = redis_keys.first.split(':').first
      expect(key_prefix).to eq('override')
    end

    it 'overriding the first part of the key prefix in one class does not affect another class' do
      DefaultKeyPrefix.new(redis, pipeline_module, '1234')
      OverrideKeyPrefix.new(redis, pipeline_module, '1234')

      redis_keys = redis.keys
      expect(redis_keys.size).to eq(2)

      key_prefixes = redis_keys.map {|key| key.split(':').first}
      expect(key_prefixes).to include('plines', 'override')
    end
  end
end

