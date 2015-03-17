require 'plines'

redis_url = if File.exist?('./config/redis_connection_url.txt')
  File.read('./config/redis_connection_url.txt').strip
else
  "redis://localhost:6379/1"
end

$redis = Redis.new(url: redis_url)

module Pipeline
  extend Plines::Pipeline

  configure do |plines|
    plines.batch_list_key { "key" }

    qless = Qless::Client.new(redis: $redis)
    plines.qless_client { qless }
  end

  def self.fan_out_by_groups_and_count(klass)
    klass.fan_out do |batch_data|
      batch_data.fetch("groups").map do |group|
        batch_data.merge("group" => group)
      end
    end

    klass.fan_out do |batch_data|
      batch_data.fetch("fan_out_count").times.map do |i|
        batch_data.merge("counter" => i)
      end
    end
  end

  class RootStep
    extend Plines::Step
    depended_on_by_all_steps
    def perform; end
  end

  class Step1
    extend Plines::Step
    Pipeline.fan_out_by_groups_and_count(self)
    def perform; end
  end

  class Step2
    extend Plines::Step
    Pipeline.fan_out_by_groups_and_count(self)

    depends_on :Step1 do |data|
      data.my_data.fetch("group") == data.their_data.fetch("group")
    end

    def perform; end
  end

  class FinalStep
    extend Plines::Step
    depends_on_all_steps
    def perform; end
  end

  def self.enqueue_a_batch(size)
    enqueue_jobs_for("groups" => %w[ a b c d e ], "fan_out_count" => size)
  end
end

module Plines
  module ForceCollectionType
    def initialize(*args)
      super
      @dependencies = ForceCollectionType.collection_type.new
      @dependents   = ForceCollectionType.collection_type.new
    end

    class << self
      attr_accessor :collection_type
    end
  end

  Job.class_eval do
    prepend ForceCollectionType
  end
end

description = "Enqueing a batch of %s (%s)"
sizes = [10, 50, 100, 200]

require 'benchmark'
$redis.flushdb
implementations = [Set, Array]

Benchmark.bm((description % [sizes.last, implementations.last.name]).length) do |bm|
  implementations.each do |implementation|
    Plines::ForceCollectionType.collection_type = implementation

    sizes.each do |size|
      bm.report(description % [size, implementation.name]) do
        Pipeline.enqueue_a_batch(size)
        $redis.flushdb
      end
    end
  end
end

__END__
                                      user     system      total        real
Enqueing a batch of 10 (Set)      0.050000   0.000000   0.050000 (  0.075424)
Enqueing a batch of 50 (Set)      0.450000   0.020000   0.470000 (  0.666433)
Enqueing a batch of 100 (Set)     1.500000   0.050000   1.550000 (  2.126321)
Enqueing a batch of 200 (Set)     5.670000   0.080000   5.750000 (  7.785804)
Enqueing a batch of 10 (Array)    0.040000   0.000000   0.040000 (  0.068016)
Enqueing a batch of 50 (Array)    0.340000   0.020000   0.360000 (  0.545092)
Enqueing a batch of 100 (Array)   1.070000   0.030000   1.100000 (  1.688348)
Enqueing a batch of 200 (Array)   3.910000   0.070000   3.980000 (  6.020625)
