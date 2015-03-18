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

  class SomeStep
    extend Plines::Step

    fan_out do |batch_data|
      batch_data.fetch("fan_out_count").times.map do |i|
        batch_data.merge("counter" => i)
      end
    end

    def perform; end
  end

  def self.enqueue_a_batch(size)
    enqueue_jobs_for("fan_out_count" => size)
  end
end

module Plines
  JobEnqueuer.class_eval do
    alias enqueue_jobs_with_pipelining enqueue_jobs

    def enqueue_jobs_without_pipelining
      enqueue_steps_slice(@dependency_graph.ordered_steps)
      enqueue_external_dependency_timeouts

      self
    end

    def self.use_enqueue_jobs_implementation(suffix)
      alias_method :enqueue_jobs, :"enqueue_jobs_#{suffix}"
    end
  end
end

description = "Enqueing a batch of %s (%s)"
sizes = [100, 1000, 10000, 50000]

require 'benchmark'
$redis.flushdb
implementations = [:with_pipelining, :without_pipelining]

Benchmark.bm((description % [sizes.last, implementations.last]).length) do |bm|
  implementations.each do |implementation|
    Plines::JobEnqueuer.use_enqueue_jobs_implementation implementation

    sizes.each do |size|
      bm.report(description % [size, implementation.to_s.sub("_", " ")]) do
        Pipeline.enqueue_a_batch(size)
        $redis.flushdb
      end
    end
  end
end

__END__

                                                     user     system      total        real
Enqueing a batch of 100 (with pipelining)        0.020000   0.010000   0.030000 (  0.032995)
Enqueing a batch of 1000 (with pipelining)       0.170000   0.010000   0.180000 (  0.324020)
Enqueing a batch of 10000 (with pipelining)      1.790000   0.160000   1.950000 (  3.229281)
Enqueing a batch of 50000 (with pipelining)      9.040000   0.710000   9.750000 ( 16.199976)
Enqueing a batch of 100 (without pipelining)     0.020000   0.010000   0.030000 (  0.060967)
Enqueing a batch of 1000 (without pipelining)    0.270000   0.040000   0.310000 (  0.558026)
Enqueing a batch of 10000 (without pipelining)   2.570000   0.430000   3.000000 (  5.460877)
Enqueing a batch of 50000 (without pipelining)  12.500000   2.040000  14.540000 ( 26.523839)
