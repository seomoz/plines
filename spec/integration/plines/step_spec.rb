require 'spec_helper'
require 'plines'

module Plines
  describe Step, :redis do
    include_context "integration helpers"

    context "when performing the job" do
      let(:popped_job) { P.default_queue.pop }

      def fail_job_on_the_side(job)
        other_instance = P.qless.jobs[job.jid]
        other_instance.fail("boom", caller.join("\n"))
      end

      it 'marks the job as complete' do
        create_pipeline_with_step
        batch = enqueue_batch

        P::A.perform(popped_job)

        expect(batch.pending_job_jids.size).to eq(0)
        expect(batch.completed_job_jids.size).to eq(1)
      end

      it 'does not mark the job complete if something else failed it on the side' do
        create_pipeline_with_step
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

