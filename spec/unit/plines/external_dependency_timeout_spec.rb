require 'spec_helper'
require 'plines/external_dependency_timeout'
require 'plines/job_batch'

module Plines
  describe ExternalDependencyTimeout do
    let(:pipeline) { fire_replaced_class_double("My::Pipeline") }
    let(:job_batch) { JobBatch.create(pipeline, "abc", {}) }
    let(:job) { fire_double("Qless::Job") }

    it 'times out the named dependency for the given jobs on the given job batch' do
      data = ExternalDependencyTimeout.job_data_for(job_batch, "foo", ["a", "b"])
      job.stub(data: data)

      Plines::JobBatch.stub(:find).with(job_batch.pipeline, job_batch.id, :reconnect_to_redis) { job_batch }
      expect(job_batch).to respond_to(:timeout_external_dependency).with(2).arguments
      job_batch.should_receive(:timeout_external_dependency).with("foo", ["a", "b"])

      ExternalDependencyTimeout.perform(job)
    end
  end
end

