require 'spec_helper'
require 'plines/job_enqueuer'

module Plines
  describe JobEnqueuer, :redis do
    step_class(:A) { depends_on :B }
    step_class(:B)
    step_class(:C) { has_external_dependency :foo }

    let(:batch_data) { { "a" => "foo", "b" => 2 } }
    let(:graph) { DependencyGraph.new(P.step_classes, batch_data) }
    let(:job_batch) { JobBatch.new(pipeline_module, "foo:1") }

    let(:enqueuer) { JobEnqueuer.new(graph, job_batch) }

    before { P.configuration.batch_list_key { |data| data[:a] } }

    it 'enqueues jobs that have no dependencies with no dependencies' do
      enqueuer.enqueue_jobs

      jobs = P.default_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should =~ %w[ P::B ]
      jobs.map(&:data).should eq([{ "a" => "foo", "b" => 2, "_job_batch_id" => "foo:1" }])
    end

    it 'sets up job dependencies correctly' do
      enqueuer.enqueue_jobs

      job = P.default_queue.pop
      job.complete

      jobs = P.default_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should =~ %w[ P::A ]
      jobs.map(&:data).should eq([{ "a" => "foo", "b" => 2, "_job_batch_id" => "foo:1" }])
    end

    it 'sets up external dependencies correctly' do
      enqueuer.enqueue_jobs

      jobs = P.awaiting_external_dependency_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should eq(["P::C"])
      jobs.map(&:data).should eq([{ "a" => "foo", "b" => 2, "_job_batch_id" => "foo:1" }])

      EnqueuedJob.new(jobs.first.jid).pending_external_dependencies.should eq([:foo])
    end

    it 'adds the jids to a redis set so that the entire job batch can be easily tracked' do
      enqueuer.enqueue_jobs

      a = P.default_queue.peek(1).first
      b_jid = a.dependents.first
      job_batch.job_jids.should include(a.jid, b_jid)
    end
  end
end

