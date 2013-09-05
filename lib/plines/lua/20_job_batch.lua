local PlinesJobBatch = {}
PlinesJobBatch.__index = PlinesJobBatch

function Plines.job_batch(pipeline_name, id)
  local job_batch = {}
  setmetatable(job_batch, PlinesJobBatch)

  job_batch.pipeline_name = pipeline_name
  job_batch.id = id
  job_batch.key = "plines:" .. pipeline_name .. ":JobBatch:" .. id

  return job_batch
end

function PlinesJobBatch:expire(data_ttl_in_milliseconds)
  for _, sub_key in ipairs(plines_job_batch_sub_keys) do
    redis.call('pexpire', self.key .. ":" .. sub_key, data_ttl_in_milliseconds)
  end

  for _, jid in ipairs(self:jids()) do
    local job = Plines.enqueued_job(self.pipeline_name, jid)
    job:expire(data_ttl_in_milliseconds)

    for _, dep in ipairs(job:external_dependencies()) do
      redis.call('pexpire', self.key .. ":ext_deps:" .. dep, data_ttl_in_milliseconds)
      redis.call('pexpire', self.key .. ":timeout_job_jids:" .. dep, data_ttl_in_milliseconds)
    end
  end
end

function PlinesJobBatch:delete()
  for _, sub_key in ipairs(plines_job_batch_sub_keys) do
    redis.call('del', self.key .. ":" .. sub_key)
  end

  for _, jid in ipairs(self:jids()) do
    local job = Plines.enqueued_job(self.pipeline_name, jid)
    job:delete()

    for _, dep in ipairs(job:external_dependencies()) do
      redis.call('del', self.key .. ":ext_deps:" .. dep)
      redis.call('del', self.key .. ":timeout_job_jids:" .. dep)
    end
  end
end

function PlinesJobBatch:complete_job(jid, data_ttl_in_milliseconds, worker, now_iso8601)
  local job = Qless.job(jid)
  local job_meta = job:data()

  if redis.call("sismember", self.key .. ":pending_job_jids", jid) == 0 then
    error("JobNotPending: " .. jid)
  end

  job:complete(now, worker, job_meta['queue'], job_meta['data'])

  redis.call('smove', self.key .. ":pending_job_jids",
             self.key .. ":completed_job_jids", jid)

  if self:is_completed() then
    redis.call('hset', self.key .. ":meta", "completed_at", now_iso8601)
    self:expire(data_ttl_in_milliseconds)
  end
end

function PlinesJobBatch:is_completed()
  return redis.call('scard', self.key .. ":pending_job_jids") == 0 and
         redis.call('scard', self.key .. ":completed_job_jids") > 0
end

function PlinesJobBatch:jids()
  return redis.call('sunion',
    self.key .. ":completed_job_jids",
    self.key .. ":pending_job_jids"
  )
end
