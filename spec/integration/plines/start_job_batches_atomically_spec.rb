require 'plines'
require 'delegate'

RSpec.describe "Starting job batches atomically", :redis do
  module MyPipeline
    extend Plines::Pipeline

    class MyStep
      extend Plines::Step

      fan_out do |data|
        1.upto(data[:count]).map do |i|
          { counter: i, simulate_timeout: (i == 3) }
        end
      end

      def perform; end
    end
  end

  class FlakeyRedis < DelegateClass(Redis)
    def evalsha(sha, argv:, **rest)
      return super unless argv.first == "put"

      data = JSON.parse(argv[6])
      if data["simulate_timeout"]
        raise ::Redis::TimeoutError
      else
        super
      end
    end
  end

  let(:qless) { Qless::Client.new(redis: FlakeyRedis.new(redis)) }

  before do
    MyPipeline.configure do |plines|
      plines.batch_list_key { "key" }
      plines.qless_client   { qless }
    end
  end

  def silence_warnings(&block)
    expect(&block).to output(anything).to_stdout_from_any_process.and output(anything).to_stderr_from_any_process
  end

  it 'does not let any jobs run until the entire set of job batches have been fully created' do
    expect {
      silence_warnings do
        MyPipeline.start_job_batches_atomically do |atomic_enqueuer|
          atomic_enqueuer.enqueue_jobs_for(count: 2)
          atomic_enqueuer.enqueue_jobs_for(count: 3)
        end
      end
    }.to raise_error Redis::TimeoutError

    jbs = MyPipeline.job_batch_list_for("key").to_a
    expect(jbs.size).to eq(2)
    expect(jbs).to all have_attributes(paused?: true)

    MyPipeline.start_job_batches_atomically do |atomic_enqueuer|
      atomic_enqueuer.enqueue_jobs_for(count: 2)
      atomic_enqueuer.enqueue_jobs_for(count: 2)
    end

    jbs = MyPipeline.job_batch_list_for("key").to_a - jbs
    expect(jbs).to all have_attributes(paused?: false)
  end
end
