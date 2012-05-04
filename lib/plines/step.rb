module Plines
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
      pipeline.root_dependency = self
    end

    def depends_on_all_steps
      extend DependsOnAllSteps
    end

    def fan_out(&block)
      @fan_out_block = block
    end

    def has_external_dependency(*external_deps)
      external_dependencies.merge(external_deps)
    end

    def has_external_dependencies?
      external_dependencies.any?
    end

    def dependencies_for(job, batch_data)
      Enumerator.new do |yielder|
        has_dependencies = false

        each_declared_dependency_job_for(job, batch_data) do |job|
          has_dependencies = true
          yielder.yield job
        end

        each_root_dependency_job_for(job, batch_data) do |job|
          yielder.yield job
        end unless has_dependencies
      end
    end

    def jobs_for(batch_data)
      @fan_out_block.call(batch_data).map do |job_data|
        Job.build(self, job_data)
      end
    end

    def perform(qless_job)
      batch = JobBatch.new(pipeline, qless_job.data.fetch("_job_batch_id"))
      job_data = DynamicStruct.new(qless_job.data.reject do |k, v|
        k == "_job_batch_id"
      end)

      new(batch, job_data).send(:around_perform)

      batch.mark_job_as_complete(qless_job.jid)
    end

    def external_dependencies
      @external_dependencies ||= Set.new
    end

    def qless_queue
      external_dependencies.any? ?
        pipeline.awaiting_external_dependency_queue :
        pipeline.default_queue
    end

  private

    def pipeline
      @pipeline ||= begin
        namespaces = name.split('::')
        namespaces.pop # ignore the last one
        namespaces.inject(Object) { |ns, mod| ns.const_get(mod) }
      end
    end

    def dependency_filters
      @dependency_filters ||= {}
    end

    def default_dependency_filter
      Proc.new { true }
    end

    def each_declared_dependency_job_for(job, batch_data)
      dependency_filters.each do |klass, filter|
        klass = pipeline.const_get(klass) unless klass.is_a?(Class)
        klass.jobs_for(batch_data).each do |dependency|
          yield dependency if filter[job.data, dependency.data]
        end
      end
    end

    def each_root_dependency_job_for(job, batch_data)
      return if pipeline.root_dependency == self

      pipeline.root_dependency.jobs_for(batch_data).each do |dependency|
        yield dependency
      end
    end

    module InstanceMethods
      attr_reader :job_data, :job_batch

      def initialize(job_batch, job_data)
        @job_batch = job_batch
        @job_data = job_data
      end

    private

      def around_perform
        perform
      end
    end

    module DependsOnAllSteps
    private
      def dependency_filters
        klasses = pipeline.step_classes.reject { |c| c == self }
        Hash[ *klasses.flat_map { |c| [c, default_dependency_filter] } ]
      end
    end
  end
end

