require 'benchmark'
require 'bundler/setup'
require 'plines'


module MyPipeline
  extend Plines::Pipeline

  configure do |plines|
    plines.batch_list_key { "key" }

    redis_url = if File.exist?('./config/redis_connection_url.txt')
      File.read('./config/redis_connection_url.txt').strip
    else
      "redis://localhost:6379/1"
    end

    redis = Redis.new(url: redis_url)
    qless = Qless::Client.new(redis: redis)
    plines.qless_client { qless }
  end

  class Step1
    extend Plines::Step

    fan_out do |batch_data|
      batch_data.fetch(:size).times.map do |i|
        { num: i }
      end
    end
  end

  class ValidateStep1
    extend Plines::Step
    depends_on :Step1
  end

  class Step2
    extend Plines::Step

    fan_out do |batch_data|
      batch_data.fetch(:size).times.map do |i|
        { num: i }
      end
    end
  end

  class ValidateStep2
    extend Plines::Step
    depends_on :Step2
  end
end

description = "Enqueing a batch of %s"
sizes = [10, 100, 1000]

Benchmark.bm((description % sizes.last).length) do |bm|
  sizes.each do |size|
    bm.report(description % size) do
      MyPipeline.enqueue_jobs_for(size: size)
    end
  end
end

=begin
Original times (using naive DFS):
                               user     system      total        real
Enqueing a batch of 10     0.030000   0.010000   0.040000 (  0.031085)
Enqueing a batch of 100    0.150000   0.010000   0.160000 (  0.223950)
Enqueing a batch of 1000   1.650000   0.130000   1.780000 (  2.302887)
=end

