module Plines
  # This is the job class that we pass to Qless to perform. It takes care
  # of delegating to the individual plines job.
  module JobPerformer
    extend self

    def perform(job)
      step_class = step_class_for(job)
      job_instance = step_class.new(job.data.fetch("data", {}))
      job_instance.perform
    end

  private

    def step_class_for(job)
      klass_name = job.data["klass"]
      klass_name.split('::').inject(Object) { |ns, name| ns.const_get(name) }
    end
  end
end

