require 'spec_helper'
require 'plines/pipeline'
require 'plines/configuration'

require 'plines/redis_objects'

module Plines
  describe RedisObjectsHelpers, :redis do
    class DefaultKeyPrefix < Struct.new(:pipeline, :id)
      include Plines::RedisObjectsHelpers
      counter :a_counter
      attr_reader :redis

      def initialize(pipeline, id)
        super(pipeline, id)
        @redis = pipeline.redis
        a_counter.increment # have to increment to create the key
      end
    end

    class OverrideKeyPrefix < DefaultKeyPrefix
      redis_key_prefix 'override'
    end

    it 'contains the pipeline name, class name, and id in the key prefix' do
      DefaultKeyPrefix.new(pipeline_module, '1234')

      redis_keys = pipeline_module.redis.keys
      expect(redis_keys).to have(1).thing

      key_prefix_parts = redis_keys.first.split(':')
      expect(key_prefix_parts).to include('P', 'DefaultKeyPrefix', '1234')
    end

    it 'uses `plines` as the first part of the key prefix by default' do
      DefaultKeyPrefix.new(pipeline_module, '1234')

      redis_keys = pipeline_module.redis.keys
      expect(redis_keys).to have(1).thing

      key_prefix = redis_keys.first.split(':').first
      expect(key_prefix).to eq('plines')
    end

    it 'can override the first part of the key prefix' do
      OverrideKeyPrefix.new(pipeline_module, '1234')

      redis_keys = pipeline_module.redis.keys
      expect(redis_keys).to have(1).thing

      key_prefix = redis_keys.first.split(':').first
      expect(key_prefix).to eq('override')
    end

    it 'overriding the first part of the key prefix in one class does not affect another class' do
      DefaultKeyPrefix.new(pipeline_module, '1234')
      OverrideKeyPrefix.new(pipeline_module, '1234')

      redis_keys = pipeline_module.redis.keys
      expect(redis_keys).to have(2).things

      key_prefixes = redis_keys.map {|key| key.split(':').first}
      expect(key_prefixes).to include('plines', 'override')
    end
  end
end

