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

  class Step2
    extend Plines::Step

    fan_out do |batch_data|
      batch_data.fetch(:size).times.map do |i|
        { num: i }
      end
    end
  end
end

pipeline = MyPipeline
batch_data = { size: 1000 }
step_classes = pipeline.step_classes

Benchmark.benchmark do |bm|
  bm.report do
    @steps = Plines::Job.accumulate_instances do
      step_classes.each do |step_klass|
        step_klass.jobs_for(batch_data).each do |job|
          job.add_dependencies_for(batch_data)
        end
      end

      @terminal_jobs = pipeline.terminal_step.jobs_for(batch_data)
    end
  end
end

=begin
Before making any changes:

$ ruby benchmarks/accumulating_job_instances.rb
    7.410000   0.020000   7.430000 (  7.445781)

After my fix:

$ ruby benchmarks/accumulating_job_instances.rb
    0.060000   0.000000   0.060000 (  0.063829)
=end

