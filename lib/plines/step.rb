require 'plines/dependency_graph'
require 'plines/job'

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

    attr_reader :job_data

    def initialize(job_data)
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

