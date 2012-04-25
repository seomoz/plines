require 'spec_helper'
require 'plines/job_enqueuer'

module Plines
  describe JobEnqueuer, :redis do
    step_class(:A) { depends_on :B }
    step_class(:B)
    let(:graph) { graph = Plines::Step.to_dependency_graph(a: 1, b:2) }
    let(:enqueuer) { JobEnqueuer.new(graph) }

    it 'enqueues jobs that have no dependencies with no dependencies' do
      enqueuer.enqueue_jobs

      jobs = Plines.default_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should =~ %w[ B ]
      jobs.map(&:data).should eq([{ "a" => 1, "b" => 2 }])
    end

    it 'sets up job dependencies correctly' do
      enqueuer.enqueue_jobs

      job = Plines.default_queue.pop
      job.complete

      jobs = Plines.default_queue.peek(2)
      jobs.map { |j| j.klass.to_s }.should =~ %w[ A ]
      jobs.map(&:data).should eq([{ "a" => 1, "b" => 2 }])
    end
  end
end

