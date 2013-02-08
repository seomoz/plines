module Plines
  # This is a job that gets enqueued with a delay in order to timeout external
  # dependencies.  When it runs, it will timeout the named external dependency
  # for the given jids.  If the named dependency is the only remaining pending
  # dependency for any of the jobs identified by the jids, they will get moved
  # into their appropriate processing queue.
  class ExternalDependencyTimeout
    def self.perform(job)
      pipeline_parts = job.data.fetch('pipeline').split('::')
      pipeline = pipeline_parts.inject(Object) { |ns, mod| ns.const_get(mod) }

      job_batch = JobBatch.find(job.client, pipeline,
                                job.data.fetch("job_batch_id"))

      job_batch.timeout_external_dependency \
        job.data.fetch("dep_name"),
        job.data.fetch("jids")
    end

    def self.job_data_for(job_batch, dep_name, jids)
      {
        "pipeline" => job_batch.pipeline.name,
        "job_batch_id" => job_batch.id,
        "dep_name" => dep_name,
        "jids" => jids
      }
    end
  end
end

