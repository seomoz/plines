local PlinesEnqueuedJob = {}
PlinesEnqueuedJob.__index = PlinesEnqueuedJob

function Plines.enqueued_job(pipeline_name, jid)
  local job = {}
  setmetatable(job, PlinesEnqueuedJob)

  job.pipeline_name = pipeline_name
  job.jid = jid
  job.key = "plines:" .. pipeline_name .. ":EnqueuedJob:" .. jid

  return job
end

function PlinesEnqueuedJob:expire(data_ttl_in_milliseconds)
  for _, sub_key in ipairs(plines_enqueued_job_sub_keys) do
    redis.call('pexpire', self.key .. ":" .. sub_key, data_ttl_in_milliseconds)
  end
end

function PlinesEnqueuedJob:external_dependencies()
  return redis.call('sunion',
    self.key .. ":pending_ext_deps",
    self.key .. ":resolved_ext_deps",
    self.key .. ":timed_out_ext_deps"
  )
end

