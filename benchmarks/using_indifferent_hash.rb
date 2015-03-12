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

description = "Enqueing a batch of %s (with `expose_indifferent_hashes = %s`)"
sizes = [10, 50, 100]

require 'benchmark'

Benchmark.bm((description % [sizes.last, false]).length) do |bm|
  sizes.each do |size|
    [true, false].each do |expose_indifferent_hashes|
      bm.report(description % [size, expose_indifferent_hashes]) do
        LargePipeline.configuration.expose_indifferent_hashes = expose_indifferent_hashes
        LargePipeline.enqueue_a_batch(size)
        $redis.flushdb
      end
    end
  end
end

__END__

                                                                        user     system      total        real
Enqueing a batch of 10 (with `expose_indifferent_hashes = true`)     0.240000   0.010000   0.250000 (  0.277971)
Enqueing a batch of 10 (with `expose_indifferent_hashes = false`)    0.080000   0.010000   0.090000 (  0.125868)
Enqueing a batch of 50 (with `expose_indifferent_hashes = true`)     4.630000   0.050000   4.680000 (  4.950930)
Enqueing a batch of 50 (with `expose_indifferent_hashes = false`)    1.470000   0.030000   1.500000 (  1.748757)
Enqueing a batch of 100 (with `expose_indifferent_hashes = true`)   18.550000   0.110000  18.660000 ( 19.605585)
Enqueing a batch of 100 (with `expose_indifferent_hashes = false`)   5.650000   0.080000   5.730000 (  6.522413)
