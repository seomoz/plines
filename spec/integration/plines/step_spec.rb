require 'spec_helper'
require 'plines'

module Plines
  describe Step, :redis do
    context "when performing the job" do
      let(:popped_job) { P.default_queue.pop }

      def create_pipeline
        step_class(:A) do
          def perform; end # no-op
        end

        P.configure do |config|
          config.batch_list_key { |hash| hash[:id] }
        end
      end

      def enqueue_batch
        P.enqueue_jobs_for(id: 1)
        P.most_recent_job_batch_for(id: 1).tap do |batch|
          expect(batch.job_jids.size).to eq(1)
        end
      end

      def fail_job_on_the_side(job)
        other_instance = P.qless.jobs[job.jid]
        other_instance.fail("boom", caller.join("\n"))
      end

      it 'marks the job as complete' do
        create_pipeline
        batch = enqueue_batch

        P::A.perform(popped_job)

        expect(batch.pending_job_jids.size).to eq(0)
        expect(batch.completed_job_jids.size).to eq(1)
      end

      it 'does not mark the job complete if something else failed it on the side' do
        create_pipeline
        batch = enqueue_batch

        job = popped_job
        fail_job_on_the_side(batch.qless_jobs.first)

        begin
          P::A.perform(job)
        rescue Qless::Job::CantCompleteError
          # the job can't be completed when it has already failed
        end

        expect(batch.pending_job_jids.size).to eq(1)
        expect(batch.completed_job_jids.size).to eq(0)
      end
    end
  end
end

