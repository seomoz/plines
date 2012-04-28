require 'plines/dependency_graph'
require 'plines/job'
require 'plines/dynamic_struct'

module Plines
  # This is the module that should be included in any class that
  # is intended to be a Plines step.
  module Step
    def self.all_classes
      @all_classes ||= []
    end

    def self.included(klass)
      klass.extend ClassMethods
      klass.fan_out { |d| [d] } # default to one step instance
      Plines::Step.all_classes << klass
    end

    attr_reader :job_data, :job_batch

    def initialize(job_batch, job_data)
      @job_batch = job_batch
      @job_data = job_data
    end

    # The class-level Plines step macros.
    module ClassMethods
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
            klass = module_namespace.const_get(klass)
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
        job_batch = JobBatch.new(qless_job.data.fetch("_job_batch_id"))
        job_data = DynamicStruct.new(qless_job.data.reject do |k, v|
          k == "_job_batch_id"
        end)

        new(job_batch, job_data).perform

        job_batch.mark_job_as_complete(qless_job.jid)
      end

      def external_dependencies
        @external_dependencies ||= Set.new
      end

    private

      def module_namespace
        namespaces = name.split('::')
        namespaces.pop # ignore the last one
        namespaces.inject(Object) { |ns, mod| ns.const_get(mod) }
      end

      def dependency_filters
        @dependency_filters ||= {}
      end
    end
  end
end

