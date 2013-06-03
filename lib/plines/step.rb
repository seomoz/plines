require 'forwardable'

module Plines
  ExternalDependency = Struct.new(:name, :options)

  # Keeps track of a list of external dependencies.
  # These are yielded as the first argument to
  # `has_external_dependencies`.
  class ExternalDependencyList
    extend Forwardable
    def_delegators :@dependencies, :any?

    def initialize
      @dependencies = []
    end

    def add(name, options = {})
      @dependencies << ExternalDependency.new(name, options)
    end

    def to_a
      @dependencies.dup
    end
  end

  # This is the module that should be included in any class that
  # is intended to be a Plines step.
  module Step
    # Error raised when you include Plines::Step in a module that is
    # not nested within a pipeline module.
    class NotDeclaredInPipelineError < StandardError; end

    def self.extended(klass)
      klass.class_eval do
        include InstanceMethods

        unless pipeline.is_a?(Plines::Pipeline)
          raise NotDeclaredInPipelineError,
            "#{klass} is not nested in a pipeline module and thus " +
            "cannot be a Plines::Step. All plines steps must be " +
            "declared within pipeline modules."
        end

        fan_out { |d| [d] } # default to one step instance
        pipeline.step_classes << self
      end
    end

    def depends_on(*klasses, &block)
      klasses.each do |klass|
        dependency_filters[klass] = (block || default_dependency_filter)
      end
    end

    def depended_on_by_all_steps
      pipeline.initial_step = self
    end

    def depends_on_all_steps
      pipeline.terminal_step = self
    end

    def fan_out(&block)
      @fan_out_blocks ||= []
      @fan_out_blocks << block
    end

    def has_external_dependencies(*args, &block)
      block ||= begin
        options = args.last.is_a?(Hash) ? args.pop : {}
        lambda do |deps, _|
          args.each do |name|
            deps.add name, options
          end
        end
      end

      external_dependency_definitions << block
    end

    def has_external_dependencies_for?(data)
      external_dependency_definitions.any? do |block|
        list = ExternalDependencyList.new
        block.call(list, data)
        list.any?
      end
    end

    def external_dependencies_for(data)
      list = ExternalDependencyList.new

      external_dependency_definitions.each do |block|
        block.call(list, data)
      end

      list.to_a
    end

    def dependencies_for(job, batch_data)
      Enumerator.new do |yielder|
        has_dependencies = false

        each_declared_dependency_job_for(job, batch_data) do |job|
          has_dependencies = true
          yielder.yield job
        end

        each_initial_step_job_for(job, batch_data) do |job|
          yielder.yield job
        end unless has_dependencies
      end
    end

    def jobs_for(batch_data)
      @fan_out_blocks.inject([batch_data]) do |job_data_hashes, fan_out_block|
        job_data_hashes.flat_map { |job_data| fan_out_block.call(job_data) }
      end.map do |job_data|
        Job.build(self, job_data)
      end
    end

    def perform(qless_job)
      batch = JobBatch.find(qless_job.client, pipeline,
                            qless_job.data.fetch("_job_batch_id"))
      job_data = DynamicStruct.new(qless_job.data)

      qless_job.after_complete do
        batch.mark_job_as_complete(qless_job.jid)
      end

      new(batch, job_data, qless_job)
        .send(:around_perform)
    end

    def external_dependency_definitions
      @external_dependency_definitions ||= []
    end

    def qless_options
      @qless_options ||= QlessJobOptions.new
      yield @qless_options if block_given?
      @qless_options
    end

    def enqueue_qless_job(qless, data, options = {})
      queue_name = if has_external_dependencies_for?(data)
        Pipeline::AWAITING_EXTERNAL_DEPENDENCY_QUEUE
      elsif options[:queue] && processing_queue == :plines
        options[:queue]
      else
        processing_queue
      end

      queue = qless.queues[queue_name]

      options[:priority] = qless_options.priority if qless_options.priority
      options[:priority] ||= 0
      options[:retries] = qless_options.retries if qless_options.retries
      options[:retries] ||= 0
      options[:tags] = Array(options[:tags]) | qless_options.tags

      queue.put(self, data, options)
    end

    def processing_queue
      qless_options.queue
    end

    def pipeline
      @pipeline ||= begin
        namespaces = name.split('::')
        namespaces.pop # ignore the last one
        namespaces.inject(Object) { |ns, mod| ns.const_get(mod) }
      end
    end

    def step_name
      @step_name ||= name.split('::').last.to_sym
    end

  private

    def dependency_filters
      @dependency_filters ||= {}
    end

    def default_dependency_filter
      Proc.new { true }
    end

    def each_declared_dependency_job_for(job, batch_data)
      dependency_filters.each do |klass, filter|
        klass = pipeline.const_get(klass)
        klass.jobs_for(batch_data).each do |dependency|
          yield dependency if filter[job.data, dependency.data]
        end
      end
    end

    def each_initial_step_job_for(job, batch_data)
      return if pipeline.initial_step == self

      pipeline.initial_step.jobs_for(batch_data).each do |dependency|
        yield dependency
      end
    end

    module InstanceMethods
      extend Forwardable
      attr_reader :job_data, :job_batch, :qless_job
      def_delegator "self.class", :enqueue_qless_job

      def initialize(job_batch, job_data, qless_job)
        @job_batch = job_batch
        @job_data = job_data
        @qless_job = qless_job
        @enqueued_job = EnqueuedJob.new(qless_job.client,
                                        self.class.pipeline, qless_job.jid)
      end

    private

      def around_perform
        perform
      end

      def unresolved_external_dependencies
        @unresolved_external_dependencies ||=
          @enqueued_job.unresolved_external_dependencies
      end
    end

    QlessJobOptions = Struct.new(:tags, :priority, :queue, :retries) do
      def initialize
        super
        self.queue ||= :plines
        self.tags ||= []
      end

      def tag=(value)
        self.tags = Array(value)
      end
    end
  end
end

