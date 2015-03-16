require 'plines'

redis_url = if File.exist?('./config/redis_connection_url.txt')
  File.read('./config/redis_connection_url.txt').strip
else
  "redis://localhost:6379/1"
end

$redis = Redis.new(url: redis_url)

module LargePipeline
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
    LargePipeline.fan_out_by_groups_and_count(self)
    def perform; end
  end

  class Step2
    extend Plines::Step
    LargePipeline.fan_out_by_groups_and_count(self)

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
  module Step
    class DependencyEnumerator
      module ToggleImplementations
        class << self
          attr_accessor :use_old_implementation
        end

        class HashWithAliasedFetch < Hash
          alias fetch []
        end

        def initialize(job, batch_data, jobs_by_klass)
          if ToggleImplementations.use_old_implementation
            jobs_by_klass = HashWithAliasedFetch.new { |_, klass| klass.jobs_for(batch_data) }
            super(job, batch_data, jobs_by_klass)
          else
            super
          end
        end
      end

      prepend ToggleImplementations
    end
  end
end

description = "Enqueing a batch of %s (with %s DependencyEnumerator impl and indif_hashes = %s)"
sizes = [10, 50, 100]

require 'benchmark'
$redis.flushdb

Benchmark.bm((description % [sizes.last, 'new', false]).length) do |bm|
  sizes.each do |size|
    ['old', 'new'].each do |dep_enum_implementation|
      [true, false].each do |expose_indifferent_hashes|
        bm.report(description % [size, dep_enum_implementation, expose_indifferent_hashes]) do
          Plines::Step::DependencyEnumerator::ToggleImplementations.use_old_implementation = (dep_enum_implementation == 'old')
          LargePipeline.configuration.expose_indifferent_hashes = expose_indifferent_hashes
          LargePipeline.enqueue_a_batch(size)
          $redis.flushdb
        end
      end
    end
  end
end

__END__

                                                                                            user     system      total        real
Enqueing a batch of 10 (with old DependencyEnumerator impl and indif_hashes = true)     0.220000   0.010000   0.230000 (  0.266934)
Enqueing a batch of 10 (with old DependencyEnumerator impl and indif_hashes = false)    0.080000   0.000000   0.080000 (  0.127011)
Enqueing a batch of 10 (with new DependencyEnumerator impl and indif_hashes = true)     0.070000   0.010000   0.080000 (  0.108826)
Enqueing a batch of 10 (with new DependencyEnumerator impl and indif_hashes = false)    0.060000   0.010000   0.070000 (  0.105372)
Enqueing a batch of 50 (with old DependencyEnumerator impl and indif_hashes = true)     4.570000   0.040000   4.610000 (  4.890019)
Enqueing a batch of 50 (with old DependencyEnumerator impl and indif_hashes = false)    1.440000   0.030000   1.470000 (  1.726128)
Enqueing a batch of 50 (with new DependencyEnumerator impl and indif_hashes = true)     0.950000   0.030000   0.980000 (  1.246436)
Enqueing a batch of 50 (with new DependencyEnumerator impl and indif_hashes = false)    0.560000   0.020000   0.580000 (  0.840614)
Enqueing a batch of 100 (with old DependencyEnumerator impl and indif_hashes = true)   18.090000   0.100000  18.190000 ( 19.023288)
Enqueing a batch of 100 (with old DependencyEnumerator impl and indif_hashes = false)   5.580000   0.070000   5.650000 (  6.417238)
Enqueing a batch of 100 (with new DependencyEnumerator impl and indif_hashes = true)    3.410000   0.050000   3.460000 (  4.232371)
Enqueing a batch of 100 (with new DependencyEnumerator impl and indif_hashes = false)   1.890000   0.050000   1.940000 (  2.683225)
