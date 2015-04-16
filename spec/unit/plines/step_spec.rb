require 'plines/configuration'
require 'plines/step'
require 'plines/pipeline'
require 'plines/job_batch'
require 'plines/dynamic_struct'
require 'plines/job'
require 'plines/configuration'
require 'plines/enqueued_job'
require 'plines/dependency_graph'

module Plines
  RSpec.describe ExternalDependencyList do
    describe "#to_a" do
      it 'protects against user side effects on the returned array' do
        list = ExternalDependencyList.new
        expect {
          list.to_a << :something
        }.not_to change { list.to_a }
      end
    end
  end

  RSpec.describe Step do
    context 'when extended onto a class' do
      it "adds the class to the pipeline's list of step classes" do
        mod = Module.new
        stub_const("MyPipeline", mod)
        mod.extend Plines::Pipeline

        expect(MyPipeline.step_classes).to eq([])

        class MyPipeline::A
          extend Plines::Step
        end

        class MyPipeline::B
          extend Plines::Step
        end

        expect(MyPipeline.step_classes).to eq([MyPipeline::A, MyPipeline::B])
      end

      it 'raises an error if it is not nested in a pipeline' do
        mod = Module.new
        stub_const("MyNonPipeline", mod)

        class MyNonPipeline::A; end

        expect { MyNonPipeline::A.extend Plines::Step }.to raise_error(/not nested in a pipeline module/)
      end
    end

    describe '#step_name' do
      it 'returns the class name relative to the pipeline' do
        step_class(:A); step_class(:B)
        expect(P::A.step_name).to eq(:A)
        expect(P::B.step_name).to eq(:B)
      end
    end

    describe "#jobs_for" do
      it 'returns just 1 instance w/ the given data by default' do
        step_class(:A)
        instances = P::A.jobs_for('a' => 1)
        expect(instances.map(&:klass)).to eq([P::A])
        expect(instances.map(&:data)).to eq(['a' => 1])
      end

      it 'returns one instance per array entry returned by a fan_out block' do
        step_class(:A) do
          fan_out do |data|
            [ { "a" => data["a"] + 1 }, { "a" => data["a"] + 2 } ]
          end
        end

        instances = P::A.jobs_for("a" => 3)
        expect(instances.map(&:klass)).to eq([P::A, P::A])
        expect(instances.map(&:data)).to eq([{ 'a' => 4 }, { 'a' => 5 }])
      end

      it 'processes each fan_out block in the order they appear' do
        step_class(:A) do
          fan_out do |data|
            [ { "a" => data["a"] + 10 }, { "a" => data["a"] + 20 } ]
          end

          fan_out do |data|
            [ { "a" => data["a"] + 100 }, { "a" => data["a"] + 200 } ]
          end
        end

        instances = P::A.jobs_for('a' => 1)
        expect(instances.map(&:klass)).to eq([P::A] * 4)
        expect(instances.map(&:data)).to eq([
         { 'a' => 111 }, { 'a' => 211 }, { 'a' => 121 }, { 'a' => 221 }
        ])
      end

      it 'allows fan_out blocks to conditionally eliminate jobs' do
        step_class(:A) do
          fan_out do |data|
            [ { "a" => data["a"], "b" => 0 }, { "a" => data["a"] + 2, "c" => 0 } ]
          end

          fan_out do |data|
            if data.has_key?("b")
              []
            else
              [data]
            end
          end
        end

        instances = P::A.jobs_for('a' => 1)
        expect(instances.map(&:klass)).to eq([P::A])
        expect(instances.map(&:data)).to eq([ 'a' => 3, 'c' => 0 ])
      end
    end

    describe "#dependencies_for" do
      def job_for(klass)
        Plines::Job.send(:new, klass, data: { "a" => 3 })
      end

      it "returns an empty array for a step with no declared dependencies" do
        step_class(:StepFoo)
        expect(dependencies_for(P::StepFoo).to_a).to eq([])
      end

      it "includes the inital step if there are no other declared dependencies" do
        step_class(:Foo)
        step_class(:Bar)

        P.initial_step = P::Bar
        expect(dependencies_for(P::Foo).map(&:klass)).to eq([P::Bar])
      end

      it "does not include the initial step if there are other declared depenencies" do
        step_class(:Foo) { depends_on :Bar }
        step_class(:Bar)
        step_class(:Bazz)

        P.initial_step = P::Bazz
        expect(dependencies_for(P::Foo).map(&:klass)).not_to include(P::Bazz)
      end

      it "does not include the initial step if it is the initial step" do
        step_class(:Foo)
        step_class(:Bar)

        P.initial_step = P::Bar
        expect(dependencies_for(P::Bar).map(&:klass)).to eq([])
      end

      it "sets the pipeline's terminal_step to itself `#depends_on_all_steps` is declared" do
        step_class(:Foo) { depends_on_all_steps }

        expect(P.terminal_step).to be(P::Foo)
      end
    end

    describe "#has_external_dependencies_for?" do
      it "returns true for a step class that has external dependencies" do
        step_class(:StepA) { has_external_dependencies { |deps| deps.add "foo" } }
        expect(P::StepA.has_external_dependencies_for?(any: 'data')).to be true
      end

      it "returns false for a step class that lacks external dependencies" do
        step_class(:StepA)
        expect(P::StepA.has_external_dependencies_for?(any: 'data')).to be false
      end

      context 'for a step that has external dependencies for only some instances' do
        step_class(:StepA) do
          has_external_dependencies do |deps, job_data|
            deps.add "foo" if job_data[:depends_on_foo]
          end
        end

        it 'returns true if the block adds a dependency for the job data' do
          expect(P::StepA.has_external_dependencies_for?(depends_on_foo: true)).to be true
        end

        it 'returns false if the block does not add a dependency for the job data' do
          expect(P::StepA.has_external_dependencies_for?(depends_on_foo: false)).to be false
        end
      end
    end

    it 'supports a terse syntax for declaring external dependencies' do
      step_class(:A) do
        has_external_dependencies "Foo", "Bar", wait_up_to: 30
      end

      deps = P::A.external_dependencies_for(some: "data")
      expect(deps.map(&:name)).to match_array(%w[ Foo Bar ])
      expect(deps.map(&:options)).to eq([wait_up_to: 30] * 2)
    end

    describe "#processing_queue_for", :redis do
      it 'returns the configured queue name' do
        step_class(:A) do
          qless_options do |qless|
            qless.queue = "special"
          end
        end

        expect(P::A.processing_queue_for({})).to eq("special")
      end

      it 'passes through the provided data to the qless_options block so that a dynamic queue name can be used' do
        step_class(:A) do
          qless_options do |qless, data|
            qless.queue = "queue_for_#{data["type"]}"
          end
        end

        expect(P::A.processing_queue_for("type" => "thing")).to eq("queue_for_thing")
      end

      it 'normally exposes the data passed to `qless_options` as a normal hash' do
        expect { |probe|
          step_class(:A) { qless_options(&probe) }
          P::A.processing_queue_for("type" => "thing")
        }.to yield_with_args(anything, an_instance_of(Hash))
      end

      context "when `config.expose_indifferent_hashes = true` is set" do
        it 'uses an indifferent hash for the yielded data so it can be treated consistently as having symbolic keys' do
          P.configuration.expose_indifferent_hashes = true

          expect { |probe|
            step_class(:A) { qless_options(&probe) }
            P::A.processing_queue_for("type" => "thing")
          }.to yield_with_args(anything, an_instance_of(IndifferentHash))
        end
      end
    end

    describe "#enqueue_qless_job", :redis do
      def enqueue(options = {})
        data = options.delete(:data) || {}
        jid = P::A.enqueue_qless_job(qless, data, options)
        qless.jobs[jid]
      end

      def queue(name)
        qless.queues[name]
      end

      it 'enqueues the job with the passed dependencies' do
        step_class(:A)
        jid = queue(:foo).put(P::A, {})
        job = enqueue(depends: [jid])
        expect(job.dependencies).to eq([jid])
      end

      it 'enqueues the job with the passed data' do
        step_class(:A)
        expect(enqueue(data: { "a" => 5 }).data).to eq("a" => 5)
      end

      it 'enqueues the job to the configured queue' do
        step_class(:A) do
          qless_options do |qless|
            qless.queue = "special"
          end
        end

        expect(enqueue.queue_name).to eq("special")
      end

      it 'enqueues jobs with external dependencies to the awaiting queue even if a queue is configured' do
        step_class(:A) do
          has_external_dependencies { |deps| deps.add "foo" }
          qless_options do |qless|
            qless.queue = "special"
          end
        end

        expect(enqueue.queue_name).to eq(Pipeline::AWAITING_EXTERNAL_DEPENDENCY_QUEUE)
      end

      it 'enqueues jobs with conditional external dependencies to the correct queue' do
        step_class(:A) do
          has_external_dependencies { |deps, data| deps.add "foo" if data[:ext] }
        end

        expect(enqueue(data: { ext: true }).queue_name).to eq(Pipeline::AWAITING_EXTERNAL_DEPENDENCY_QUEUE)
        expect(enqueue(data: { ext: false }).queue_name).to eq("plines")
      end

      it 'enqueues jobs with conditional external dependencies to the correct queue when a queue option is provided' do
        step_class(:A) do
          has_external_dependencies { |deps, data| deps.add "foo" if data[:ext] }
        end

        expect(enqueue(data: { ext: true }, queue: 'pipeline_queue').queue_name).to eq(Pipeline::AWAITING_EXTERNAL_DEPENDENCY_QUEUE)
        expect(enqueue(data: { ext: false }, queue: 'pipeline_queue').queue_name).to eq('pipeline_queue')
      end

      it 'enqueues the job to the queue specified in the pipeline' do
        step_class(:A)

        expect(enqueue(queue: 'pipeline_queue').queue_name).to eq('pipeline_queue')
      end

      it 'can enqueue the job multiple times into different queues' do
        step_class(:A)

        expect(enqueue(queue: 'pipeline_queue').queue_name).to eq('pipeline_queue')
        expect(enqueue(queue: 'pipeline_queue2').queue_name).to eq('pipeline_queue2')
        expect(enqueue.queue_name).to eq('plines')
      end

      it 'enqueues to the correct queue on a per request basis' do
        step_class(:A)

        expect(enqueue(queue: 'pipeline_queue').queue_name).to eq('pipeline_queue')
        expect(enqueue.queue_name).to eq('plines')
      end

      it 'enqueues the job to the queue specified in step class, overriding the pipeline queue' do
        step_class(:A) do
          qless_options do |qless|
            qless.queue = "special"
          end
        end

        expect(enqueue(queue: 'pipeline_queue').queue_name).to eq("special")
        expect(enqueue.queue_name).to eq("special")
      end

      it 'enqueues the job to the "plines" queue if no queue is configured' do
        step_class(:A)
        expect(enqueue.queue_name).to eq("plines")
      end

      it 'enqueues the job with the configured retry count' do
        step_class(:A) do
          qless_options do |qless|
            qless.retries = 9
          end
        end

        expect(enqueue.original_retries).to eq(9)
      end

      it 'enqueues the job with the passed retry count if none is configured' do
        step_class(:A)
        expect(enqueue(retries: 12).original_retries).to eq(12)
      end

      it 'enqueues the job with the default retry count if none is passed or configured' do
        step_class(:A)
        # 5 is the Qless default for retries
        expect(enqueue.original_retries).to eq(5)
      end

      it 'enqueues the job with the configured priority' do
        step_class(:A) do
          qless_options do |qless|
            qless.priority = 100
          end
        end

        expect(enqueue(priority: 12).priority).to eq(100)
      end

      it 'enqueues the job with the passed priority if none is configured' do
        step_class(:A)
        expect(enqueue(priority: 12).priority).to eq(12)
      end

      it 'enqueues the job with no priority if none is configured or passed' do
        step_class(:A)
        expect(enqueue.priority).to eq(0)
      end

      it 'enqueues the job with the set union of the configured and passed tags' do
        step_class(:A) do
          qless_options do |qless|
            qless.tags = ["mine"]
          end
        end

        expect(enqueue.tags).to eq(["mine"])
        expect(enqueue(tags: ['foo', 'mine', 'bar']).tags).to match_array %w[ foo bar mine ]
      end

      it 'also supports the singular #tag API' do
        step_class(:A) do
          qless_options do |qless|
            qless.tag = "mine"
          end
        end

        expect(enqueue.tags).to eq(["mine"])
      end

      it "returns the jid" do
        step_class(:A)
        expect(P::A.enqueue_qless_job(qless, {})).to match(/\A[a-f0-9]{32}\z/)
      end
    end

    describe "#depended_on_by_all_steps" do
      it "sets itself as the pipline's initial step" do
        step_class(:A)
        expect(P.initial_step).not_to be(P::A)
        P::A.depended_on_by_all_steps
        expect(P.initial_step).to be(P::A)
      end
    end

    describe "#inherited_dependencies_for" do
      it 'returns the first non-zero-fan-out parent dependency' do
        step_class(:A) { depends_on :B }
        step_class(:B) { depends_on :C; fan_out { [] } }
        step_class(:C) { depends_on :D; fan_out { [] } }
        step_class(:D)

        deps = P::A.inherited_dependencies_for({}, DependencyGraph.jobs_by_klass_for(P, {}))
        expect(deps.map(&:klass)).to eq([P::D])
      end
    end

    describe "#depends_on" do
      step_class(:StepA)
      step_class(:StepB)

      def job_for(klass)
        Plines::Job.send(:new, klass, data: { a: 1 })
      end

      it "adds dependencies based on the given class name" do
        step_class(:StepC) do
          depends_on :StepA, :StepB
        end

        dependencies = dependencies_for(P::StepC, { 'a' => 1 })
        expect(dependencies.map(&:klass)).to eq([P::StepA, P::StepB])
        expect(dependencies.map(&:data)).to eq([{ 'a' => 1 }, { 'a' => 1 }])
      end

      it "resolves step class names in the enclosing module" do
        pipeline = Module.new do
          extend Plines::Pipeline
        end
        stub_const("MySteps", pipeline)

        class MySteps::A
          extend Plines::Step
        end

        class MySteps::B
          extend Plines::Step
          depends_on :A
        end

        dependencies = dependencies_for(MySteps::B, {})
        expect(dependencies.map(&:klass)).to eq([MySteps::A])
      end

      context 'when depending on a fan_out step' do
        step_class(:StepX) do
          fan_out do |data|
            [1, 2, 3].map { |v| { 'a' => data['a'] + v } }
          end
        end

        it "depends on all of the step instances of the named type when it fans out into multiple instances" do
          step_class(:StepY) do
            depends_on :StepX
          end

          dependencies = dependencies_for(P::StepY, 'a' => 17)
          expect(dependencies.map(&:klass)).to eq([P::StepX, P::StepX, P::StepX])
          expect(dependencies.map(&:data)).to eq([
            { 'a' => 18 }, { 'a' => 19 }, { 'a' => 20 }
          ])
        end

        it "depends on the the subset of instances for which the block returns true when given a block" do
          step_class(:StepY) do
            depends_on(:StepX) { |data| data.their_data['a'].even? }
          end

          dependencies = dependencies_for(P::StepY, 'a' => 17)
          expect(dependencies.map(&:klass)).to eq([P::StepX, P::StepX])
          expect(dependencies.map(&:data)).to eq([{ 'a' => 18 }, { 'a' => 20 }])
        end

        it 'includes the batch data in the yielded arg' do
          arg = nil

          step_class(:StepY) do
            fan_out { |d| ['a', 'b'].map { |v| { 'b' => v } } }
            depends_on(:StepX) { |a| arg = a }
          end

          dependencies_for(P::StepY, 'a' => 17).to_a
          expect(arg.batch_data).to eq('a' => 17)
        end
      end

      describe "#perform", :redis do
        step_class(:Other)

        let(:qless_job) do
          qless.queues["jobs"].put(P::Other,
            { "foo" => "bar", "_job_batch_id" => job_batch.id },
            jid: "my-jid"
          )

          qless.queues["jobs"].pop
        end

        let(:job_batch) { JobBatch.create(qless, pipeline_module, "abc:1", {}) }
        let(:enqueued_job) { instance_double("Plines::EnqueuedJob") }

        before do
          job_batch.pending_job_jids << qless_job.jid
          allow_any_instance_of(JobBatch).to receive(:set_expiration!)
          allow(Plines::EnqueuedJob).to receive_messages(new: enqueued_job)
        end

        context "when the job batch is paused" do
          before { job_batch.pause(retry_delay: 1700) }

          it "schedules the job to be tried again later" do
            step_class(:A) do
              def perform; raise "should not be called"; end
            end

            P::A.perform(qless_job)
            reloaded = qless.jobs[qless_job.jid]
            expect(reloaded.state).to eq("scheduled")
            expect(reloaded.queue_name).to eq(qless_job.queue_name)

            expect(job_scheduled_at(reloaded)).to be_within(1).of(Time.now + 1700)
          end
        end

        it "creates an instance and calls #perform, with the job data available as a DynamicStruct from an instance method" do
          foo = nil
          step_class(:A) do
            define_method(:perform) do
              foo = job_data.foo
            end
          end

          P::A.perform(qless_job)
          expect(foo).to eq("bar")
        end

        it "makes the job_batch and qless_job available in the perform instance method" do
          j_batch = data_hash = nil
          qljob = nil
          step_class(:A) do
            define_method(:perform) do
              j_batch = self.job_batch
              data_hash = job_data.to_hash
              qljob = qless_job
            end
          end

          P::A.perform(qless_job)
          expect(j_batch).to eq(job_batch)
          expect(data_hash).to have_key("_job_batch_id")

          expect(qljob).to eq(qless_job)
        end

        it "makes the unresolved external dependencies available in the perform instance method" do
          allow(enqueued_job).to receive_messages(unresolved_external_dependencies: ["foo", "bar"])

          unresolved_ext_deps = []
          step_class(:A) do
            define_method(:perform) do
              unresolved_ext_deps = unresolved_external_dependencies
            end
          end

          P::A.perform(qless_job)
          expect(unresolved_ext_deps).to eq(["foo", "bar"])
        end

        it "marks the job as complete in the job batch" do
          expect(job_batch.pending_job_jids).to include(qless_job.jid)
          expect(job_batch.completed_job_jids).not_to include(qless_job.jid)

          step_class(:A) do
            def perform; end
          end

          P::A.perform(qless_job)
          expect(job_batch.pending_job_jids.to_a).not_to include(qless_job.jid)
          expect(job_batch.completed_job_jids.to_a).to include(qless_job.jid)
        end

        it "does not mark the job as complete in the job batch if the job was retried" do
          expect(job_batch.pending_job_jids).to include(qless_job.jid)
          expect(job_batch.completed_job_jids).not_to include(qless_job.jid)
          expect(qless_job).to receive(:retry).and_call_original

          step_class(:A) do
            def perform
              qless_job.retry
            end
          end

          P::A.perform(qless_job)
          expect(job_batch.pending_job_jids).to include(qless_job.jid)
          expect(job_batch.completed_job_jids).not_to include(qless_job.jid)
        end

        it "supports #around_perform middleware modules" do
          step_class(:A) do
            def self.order
              @order ||= []
            end

            include Module.new {
              def around_perform
                self.class.order << :before_1
                super
                self.class.order << :after_1
              end
            }

            include Module.new {
              def around_perform
                self.class.order << :before_2
                super
                self.class.order << :after_2
              end
            }

            define_method(:perform) { self.class.order << :perform }
          end

          P::A.perform(qless_job)
          expect(P::A.order).to eq([:before_2, :before_1, :perform, :after_1, :after_2])
        end
      end
    end

    def dependencies_for(klass, batch_data={})
      jobs_by_klass = DependencyGraph.jobs_by_klass_for(klass.pipeline, batch_data)
      klass.dependencies_for(job_for(klass), batch_data, jobs_by_klass)
    end
  end
end

