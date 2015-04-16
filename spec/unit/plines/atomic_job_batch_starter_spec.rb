require 'plines/atomic_job_batch_starter'

module Plines
  RSpec.describe AtomicJobBatchStarter do
    let(:pipeline) { instance_double("Plines::Pipeline") }
    let(:starter)  { described_class.new(pipeline) }

    describe "#enqueue_jobs_for" do
      it "returns the job batch to conform to the `Plines::Pipeline` interface" do
        jb = instance_double("Plines::JobBatch")
        allow(pipeline).to receive(:enqueue_jobs_for).and_return(jb)

        expect(starter.enqueue_jobs_for({})).to be jb
      end
    end

    describe "#atomically_start_created_batches" do
      def redis_double
        instance_double("Redis").tap { |r| allow(r).to receive(:multi).and_yield }
      end

      let(:redis) { redis_double }
      let(:jb1) { instance_double("Plines::JobBatch", redis: redis, unpause: nil) }
      let(:jb2) { instance_double("Plines::JobBatch", redis: redis, unpause: nil) }

      before do
        allow(pipeline).to receive(:enqueue_jobs_for).and_return(jb1, jb2)
      end

      it 'unpauses the created batches using `multi` to ensure atomicity' do
        expect(redis).to receive(:multi) do |&block|
          expect([jb1, jb2]).to all not_have_received(:unpause)

          block.call

          expect([jb1, jb2]).to all have_received(:unpause)
        end

        starter.enqueue_jobs_for({})
        starter.enqueue_jobs_for({})

        starter.atomically_start_created_batches
      end

      it 'raises a clear error if the job batches are for different redis connections since we cannot atomically unpause in that case' do
        allow(jb1).to receive(:redis).and_return(redis_double)
        allow(jb2).to receive(:redis).and_return(redis_double)

        starter.enqueue_jobs_for({})
        starter.enqueue_jobs_for({})

        expect {
          starter.atomically_start_created_batches
        }.to raise_error(AtomicJobBatchStarter::MoreThanOneRedisServerError)

        expect(jb1).not_to have_received(:unpause)
        expect(jb2).not_to have_received(:unpause)
      end

      it 'does nothing if there were no created batches' do
        expect { starter.atomically_start_created_batches }.not_to raise_error
      end
    end
  end
end
