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
        dependency_filters[klass] = (block || Proc.new { true })
      end
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

    def dependencies_for(batch_data)
      Enumerator.new do |yielder|
        dependency_filters.each do |klass, filter|
          klass = pipeline.const_get(klass)
          klass.jobs_for(batch_data).each do |job|
            yielder.yield job if filter[job.data]
          end
        end
      end
    end

    def has_no_dependencies?
      dependency_filters.none?
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

      new(batch, job_data).instance_eval do
        around_perform { perform }
      end

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

    module InstanceMethods
      attr_reader :job_data, :job_batch

      def initialize(job_batch, job_data)
        @job_batch = job_batch
        @job_data = job_data
      end

    private

      def around_perform
        yield
      end
    end
  end
end

