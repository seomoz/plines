require 'spec_helper'
require 'plines/job_enqueuer'

module Plines
  describe JobEnqueuer, :redis do
    step_class(:A) { depends_on :B }
    step_class(:B)
    step_class(:C) { has_external_dependency :foo }

    let(:enqueuer) { JobEnqueuer.new(a: "foo", b: 2) }

    before { Plines.configuration.batch_list_key { |data| data[:a] } }

    it 'enqueues jobs that have no dependencies with no dependencies' do
      enqueuer.enqueue_jobs

      jobs = Plines.default_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should =~ %w[ B ]
      jobs.map(&:data).should eq([{ "a" => "foo", "b" => 2, "_job_batch_id" => "foo:1" }])
    end

    it 'sets up job dependencies correctly' do
      enqueuer.enqueue_jobs

      job = Plines.default_queue.pop
      job.complete

      jobs = Plines.default_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should =~ %w[ A ]
      jobs.map(&:data).should eq([{ "a" => "foo", "b" => 2, "_job_batch_id" => "foo:1" }])
    end

    it 'sets up external dependencies correctly' do
      enqueuer.enqueue_jobs

      jobs = Plines.awaiting_external_dependency_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should eq(["C"])
      jobs.map(&:data).should eq([{ "a" => "foo", "b" => 2, "_job_batch_id" => "foo:1" }])
    end

    it 'adds the jids to a redis set so that the entire job batch can be easily tracked' do
      enqueuer.enqueue_jobs

      batch = JobBatchList.for(a: "foo", b: 2).most_recent_batch
      a = Plines.default_queue.peek(1).first
      b_jid = a.dependents.first
      batch.job_jids.should include(a.jid, b_jid)
    end
  end
end

