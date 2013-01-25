require 'spec_helper'
require 'plines/external_dependency_timeout'
require 'plines/job_batch'
require 'plines/pipeline'
require 'plines/configuration'

module Plines
  describe ExternalDependencyTimeout, :redis do
    let(:job_batch) { JobBatch.create(pipeline_module, "abc", {}) }
    let(:job) { fire_double("Qless::Job") }

    it 'times out the named dependency for the given jobs on the given job batch' do
      data = ExternalDependencyTimeout.job_data_for(job_batch, "foo", ["a", "b"])
      job.stub(data: data)

      Plines::JobBatch.stub(:find).with(job_batch.pipeline, job_batch.id) { job_batch }
      expect(job_batch).to respond_to(:timeout_external_dependency).with(2).arguments
      job_batch.should_receive(:timeout_external_dependency).with("foo", ["a", "b"])

      ExternalDependencyTimeout.perform(job)
    end
  end
end

