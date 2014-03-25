require 'spec_helper'
require 'plines/external_dependency_timeout'
require 'plines/job_batch'
require 'plines/pipeline'
require 'plines/configuration'

module Plines
  describe ExternalDependencyTimeout, :redis do
    let(:job_batch) { JobBatch.create(qless, pipeline_module, "abc", {}) }
    let(:qless_client) { instance_double("Qless::Client") }
    let(:job) { instance_double("Qless::Job", client: qless_client) }

    it 'times out the named dependency for the given jobs on the given job batch' do
      data = ExternalDependencyTimeout.job_data_for(job_batch, "foo", ["a", "b"])
      allow(job).to receive_messages(data: data)

      allow(Plines::JobBatch).to receive(:find).with(qless_client, job_batch.pipeline, job_batch.id) { job_batch }
      expect(job_batch).to respond_to(:timeout_external_dependency).with(2).arguments
      expect(job_batch).to receive(:timeout_external_dependency).with("foo", ["a", "b"])

      ExternalDependencyTimeout.perform(job)
    end
  end
end

