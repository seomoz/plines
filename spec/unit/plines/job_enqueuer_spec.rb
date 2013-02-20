require 'spec_helper'
require 'plines/pipeline'
require 'plines/step'
require 'plines/job_enqueuer'
require 'plines/enqueued_job'
require 'plines/configuration'
require 'plines/dependency_graph'
require 'plines/job'
require 'plines/job_batch'
require 'plines/external_dependency_timeout'

module Plines
  describe JobEnqueuer, :redis do
    let(:batch_data) { { "a" => "foo", "b" => 2 } }
    let(:graph) { DependencyGraph.new(P.step_classes, batch_data) }
    let(:job_batch) { JobBatch.create(qless, pipeline_module, "foo:1", {}) }

    let(:enqueuer) { JobEnqueuer.new(graph, job_batch) { |job| { tags: [job.data.fetch("a")] } } }

    before { pipeline_module.configuration.batch_list_key { |data| data[:a] } }

    let(:default_queue) { qless.queues[Pipeline::DEFAULT_QUEUE] }

    context "enqueing jobs with no timeouts" do
      step_class(:A) { depends_on :B }
      step_class(:B)
      step_class(:C) { has_external_dependencies { |deps| deps.add "foo" } }

      it 'enqueues jobs that have no dependencies with no dependencies' do
        enqueuer.enqueue_jobs

        jobs = default_queue.peek(2)
        expect(jobs.map { |j| j.klass.to_s }).to match_array %w[ P::B ]
        expect(jobs.map(&:data)).to eq([{ "a" => "foo", "b" => 2, "_job_batch_id" => "foo:1" }])
        expect(jobs.map(&:tags)).to eq([["foo"]])
      end

      it 'sets up job dependencies correctly' do
        enqueuer.enqueue_jobs

        job = default_queue.pop
        job.complete

        jobs = default_queue.peek(2)
        expect(jobs.map { |j| j.klass.to_s }).to match_array %w[ P::A ]
        expect(jobs.map(&:data)).to eq([{ "a" => "foo", "b" => 2, "_job_batch_id" => "foo:1" }])
      end

      it 'sets up external dependencies correctly' do
        enqueuer.enqueue_jobs

        jobs = qless.queues[Pipeline::AWAITING_EXTERNAL_DEPENDENCY_QUEUE].peek(2)
        expect(jobs.map { |j| j.klass.to_s }).to eq(["P::C"])
        expect(jobs.map(&:data)).to eq([{ "a" => "foo", "b" => 2, "_job_batch_id" => "foo:1" }])

        expect(EnqueuedJob.new(qless, P, jobs.first.jid).pending_external_dependencies).to eq(["foo"])
      end

      it 'adds the jids to a redis set so that the entire job batch can be easily tracked' do
        enqueuer.enqueue_jobs

        a = default_queue.peek(1).first
        b_jid = a.dependents.first
        expect(job_batch.job_jids).to include(a.jid, b_jid)
      end

      it 'adds all the jids to the job batch before actually enqueing any jobs' do
        enqueuer.should_receive(:enqueue_job_for).exactly(3).times do |job, jid, dependency_jids|
          expect(job_batch.job_jids).to include(jid, *dependency_jids)
        end

        enqueuer.enqueue_jobs
      end
    end

    describe "external_dependency timeout scheduling" do
      def scheduled_job_jids
        default_queue.jobs.scheduled
      end

      def job_for(jid)
        qless.jobs[jid]
      end

      it 'enqueues a timeout job with wait_up_to delay' do
        now  = Time.now
        Time.stub(:now) { now }

        step_class(:D) { has_external_dependencies { |deps| deps.add "bar", wait_up_to: 3000 } }
        enqueuer.enqueue_jobs

        jid = scheduled_job_jids.first
        job = job_for(jid)
        expect(job.klass.to_s).to eq("Plines::ExternalDependencyTimeout")
        expect(job.data.fetch("dep_name")).to eq("bar")
        expect(job.state).to eq("scheduled")
        expect(default_queue.peek).to be_nil

        Time.stub(:now) { now + 3001 }
        job = default_queue.peek
        expect(job.state).to eq("waiting")
        expect(job.jid).to eq(jid)
        expect(scheduled_job_jids).not_to include(jid)
      end

      it 'enqueues the timeout jobs with a high priority so that they run right away' do
        step_class(:D) { has_external_dependencies { |deps| deps.add "bar", wait_up_to: 3000 } }
        enqueuer.enqueue_jobs
        scheduled_job = job_for(scheduled_job_jids.first)
        expect(scheduled_job.priority).to eq(JobEnqueuer::TIMEOUT_JOB_PRIORITY)
      end

      it 'enqueues a single job with multiple jids if multiple steps have the same external dependency and wait_up_to setting' do
        step_class(:C) { has_external_dependencies { |deps| deps.add "bar", wait_up_to: 3000 } }
        step_class(:D) { has_external_dependencies { |deps| deps.add "bar", wait_up_to: 3000 } }
        enqueuer.enqueue_jobs

        jid, expected_nil = scheduled_job_jids.first(2)
        expect(expected_nil).to be_nil
        job = job_for(jid)
        jids = job.data.fetch("jids")
        expect(jids).to have(2).entries
        expect(jids.map { |j| job_for(j).klass.to_s }).to match_array %w[ P::C P::D ]
      end

      it 'enqueues seperate delays jobs if multiple steps have the same external dependency with different wait_up_to settings' do
        step_class(:C) { has_external_dependencies { |deps| deps.add "bar", wait_up_to: 3000 } }
        step_class(:D) { has_external_dependencies { |deps| deps.add "bar", wait_up_to: 3001 } }
        enqueuer.enqueue_jobs

        scheduled_jobs = scheduled_job_jids.map { |jid| job_for(jid) }
        expect(scheduled_jobs).to have(2).jobs
        step_jobs = scheduled_jobs.map do |j|
          jids = j.data.fetch("jids")
          expect(jids).to have(1).jid
          job_for(jids.first)
        end

        expect(step_jobs.map { |j| j.klass.to_s }).to match_array %w[ P::C P::D ]
      end

      it 'ensures the job batch is aware of all the timeout jobs so it can cancel them later' do
        step_class(:C) { has_external_dependencies "bar", wait_up_to: 3000 }
        step_class(:D) { has_external_dependencies "bar", wait_up_to: 3001 }
        step_class(:E) { has_external_dependencies "foo", wait_up_to: 3000 }
        step_class(:F) { has_external_dependencies "foo", wait_up_to: 3000 }

        enqueuer.enqueue_jobs
        expect(job_batch.timeout_job_jid_sets["bar"]).to have(2).jids
        expect(job_batch.timeout_job_jid_sets["foo"]).to have(1).jids
      end
    end
  end
end

