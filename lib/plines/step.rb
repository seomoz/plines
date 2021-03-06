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

        pipeline.step_classes << self
      end
    end

    DEFAULT_DEPENDENCY_FILTER = Proc.new { true }

    def depends_on(*klasses, &block)
      klasses.each do |klass|
        dependency_filters[klass] = (block || DEFAULT_DEPENDENCY_FILTER)
      end
    end

    def depended_on_by_all_steps
      pipeline.initial_step = self
    end

    def depends_on_all_steps
      pipeline.terminal_step = self
    end

    def fan_out(&block)
      fan_out_blocks << block
    end

    def skip_if(&block)
      fan_out_blocks << Proc.new { |data| block.call(data) ? [] : [data] }
    end

    def fan_out_blocks
      @fan_out_blocks ||= []
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

    def dependencies_for(job, batch_data, jobs_by_klass)
      DependencyEnumerator.new(job, batch_data, jobs_by_klass)
    end

    # Inherited dependencies are utilitized in place of zero-fan-out
    # direct dependencies. This is necessary so that when a job
    # depends only on another step that fans out to no job, it does
    # not wind up with no dependencies (and thus is runnable anytime)
    # but instead "inherits" the dependencies that are implicit
    # from its dependencies.
    def inherited_dependencies_for(batch_data, jobs_by_klass)
      dependency_filters.flat_map do |name, _|
        klass = pipeline.const_get(name)
        next [] if equal?(klass)

        if (jobs = jobs_by_klass.fetch(klass)).any?
          jobs
        else
          klass.inherited_dependencies_for(batch_data, jobs_by_klass)
        end
      end
    end

    # Call with care. When constructing the job batch dependency graph, we
    # want to fetch the jobs for a class from a hash of job batch lists that
    # it prepares at the start of the proceess rather than calling this and
    # producing new lists. Calling this can be expensive.
    def jobs_for(batch_data)
      fan_out_blocks.inject([batch_data]) do |job_data_hashes, fan_out_block|
        job_data_hashes.flat_map { |job_data| fan_out_block.call(job_data) }
      end.map do |job_data|
        Job.new(self, job_data)
      end
    end

    JobBatchCreationAborted = Class.new(StandardError)

    def perform(qless_job)
      batch = JobBatch.find(qless_job.client, pipeline,
                            qless_job.data.fetch("_job_batch_id"))

      if (retry_delay = batch.paused_retry_delay)
        if batch.creation_appears_to_be_stuck?
          raise JobBatchCreationAborted,
            "#{batch.inspect} appears to have been aborted during creation"
        else
          qless_job.move(qless_job.queue_name, delay: retry_delay)
          return
        end
      end

      job_data = DynamicStruct.new(qless_job.data)

      new(batch, job_data, qless_job)
        .send(:around_perform)

      batch.complete_job(qless_job) unless qless_job.state_changed?
    end

    def external_dependency_definitions
      @external_dependency_definitions ||= []
    end

    def qless_options(&block)
      @qless_options_block = block
    end

    def enqueue_qless_job(qless, data, options = {})
      qless_options = configured_qless_options_for(data)
      queue_name = initial_queue_for(qless_options.queue, data, options)
      queue = qless.queues[queue_name]

      options[:priority] = qless_options.priority if qless_options.priority
      options[:retries] = qless_options.retries if qless_options.retries
      options[:tags] = Array(options[:tags]) | qless_options.tags

      queue.put(self, data, options)
    end

    def processing_queue_for(data)
      configured_qless_options_for(data).queue
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

    def dependency_filters
      @dependency_filters ||= {}
    end

  private

    def configured_qless_options_for(data)
      QlessJobOptions.new.tap do |options|
        if @qless_options_block
          @qless_options_block.call(
            options,
            pipeline.configuration.exposed_hash_from(data)
          )
        end
      end
    end

    # Returns the queue that a job should initially be put into,
    # which will usually (but not always) be the processing queue.
    # If it has an external dependency, it's put into a waiting
    # queue first, then later moved into the processing queue.
    def initial_queue_for(processing_queue, data, options = {})
      if has_external_dependencies_for?(data)
        return Pipeline::AWAITING_EXTERNAL_DEPENDENCY_QUEUE
      end

      options_queue = options[:queue]
      return options_queue if options_queue && processing_queue == :plines

      processing_queue
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

    class DependencyEnumerator
      include Enumerable

      attr_reader :job, :batch_data, :pipeline, :jobs_by_klass

      def initialize(job, batch_data, jobs_by_klass)
        @job           = job
        @batch_data    = batch_data
        @jobs_by_klass = jobs_by_klass
        @pipeline      = job.klass.pipeline
        @zero_fan_out_dependency_steps = Set.new
      end

      def each(&block)
        dependencies.each(&block)
      end

      def dependencies
        @dependencies ||= declared_dependency_jobs +
                          initial_step_jobs +
                          transitive_dependency_jobs
      end

    private

      DependencyData = Struct.new(:my_data, :their_data, :batch_data)

      def declared_dependency_jobs
        @declared_dependency_jobs ||=
          job.klass.dependency_filters.flat_map do |name, filter|
            klass = pipeline.const_get(name)
            their_jobs = jobs_by_klass.fetch(klass)
            @zero_fan_out_dependency_steps << klass if their_jobs.none?

            their_jobs.select do |their_job|
              dep = DependencyData.new(job.data, their_job.data, batch_data)
              filter.call(dep)
            end
          end
      end

      def initial_step_jobs
        if pipeline.initial_step == job.klass
          []
        elsif declared_dependency_jobs.any?
          []
        else
          jobs_by_klass.fetch(pipeline.initial_step)
        end
      end

      def logger
        @logger ||= pipeline.configuration.logger
      end

      def transitive_dependency_jobs
        @zero_fan_out_dependency_steps.flat_map do |direct_dep|
          direct_dep.inherited_dependencies_for(
            batch_data,
            jobs_by_klass
          ).tap do |deps|
            logger.warn "Inferring implicit transitive dependency from " +
                        "#{job} for 0-fan out of #{direct_dep}: #{deps}."
          end
        end
      end
    end
  end
end

