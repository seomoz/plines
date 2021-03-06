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
  self:for_each_key_and_job(
    function(key)
      redis.call('pexpire', key, data_ttl_in_milliseconds)
    end,
    function(job)
      job:expire(data_ttl_in_milliseconds)
    end
  )
end

function PlinesJobBatch:delete()
  self:for_each_key_and_job(
    function(key)
      redis.call('del', key)
    end,
    function(job)
      job:delete()
    end
  )
end

function PlinesJobBatch:for_each_key_and_job(key_func, job_func)
  for _, sub_key in ipairs(plines_job_batch_sub_keys) do
    key_func(self.key .. ":" .. sub_key)
  end

  for _, jid in ipairs(self:jids()) do
    local job = Plines.enqueued_job(self.pipeline_name, jid)
    job_func(job)

    for _, dep in ipairs(job:external_dependencies()) do
      key_func(self.key .. ":ext_deps:" .. dep)
      key_func(self.key .. ":timeout_job_jids:" .. dep)
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

function PlinesJobBatch:timed_out_external_dependencies_set_key()
  return self.key .. ":timed_out_external_deps"
end

function PlinesJobBatch:external_dependency_set_key_for(dependency_name)
  return self.key .. ":ext_deps:" .. dependency_name
end

function PlinesJobBatch:is_awaiting_external_dependency(dependency_name)
  local dependency_is_pending   = false
  local dependency_is_timed_out = false
  local jids = redis.call('smembers', self:external_dependency_set_key_for(dependency_name))

  for _, jid in ipairs(jids) do
    local job = Plines.enqueued_job(self.pipeline_name, jid)

    if not dependency_is_pending then
      dependency_is_pending = redis.call(
        'sismember', job:pending_external_dependencies_key(), dependency_name) == 1
    end

    if not dependency_is_timed_out then
      dependency_is_timed_out = redis.call(
        'sismember', job:timed_out_external_dependencies_key(), dependency_name) == 1
    end
  end

  if dependency_is_pending and dependency_is_timed_out then
    error("InconsistentTimeoutState: Dependency " .. dependency_name .. " partially timed out.")
  end

  return dependency_is_pending and not dependency_is_timed_out
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
