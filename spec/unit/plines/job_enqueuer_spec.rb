require 'spec_helper'
require 'plines/job_enqueuer'

module Plines
  describe JobEnqueuer, :redis do
    step_class(:A) { depends_on :B }
    step_class(:B)
    let(:enqueuer) { JobEnqueuer.new(a: 1, b: 2) }

    before { Plines.configuration.batch_group_key { |data| data[:a] } }

    it 'enqueues jobs that have no dependencies with no dependencies' do
      enqueuer.enqueue_jobs

      jobs = Plines.default_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should =~ %w[ Plines::JobPerformer ]
      jobs.map(&:data).should eq([{ "klass" => "B", "data" => { "a" => 1, "b" => 2 } }])
    end

    it 'sets up job dependencies correctly' do
      enqueuer.enqueue_jobs

      job = Plines.default_queue.pop
      job.complete

      jobs = Plines.default_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should =~ %w[ Plines::JobPerformer ]
      jobs.map(&:data).should eq([{ "klass" => "A", "data" => { "a" => 1, "b" => 2 } }])
    end

    it 'adds the jids to a redis set so that the entire job batch can be easily tracked' do
      enqueuer.enqueue_jobs

      batch = Plines.job_batch_for(a: 1, b: 2)
      a = Plines.default_queue.peek(1).first
      b_jid = a.dependents.first
      batch.job_jids.should =~ [a.jid, b_jid]
    end
  end
end

