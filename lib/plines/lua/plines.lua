local Plines = {}

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

function PlinesJobBatch:complete_job(jid, data_ttl_in_milliseconds, worker, now, now_iso8601)
  local job = Qless.job(jid)
  local job_meta = job:data()
  job:complete(now, worker, job_meta['queue'], job_meta['data'])

  local moved = redis.call(
    'smove', self.key .. ":pending_job_jids",
    self.key .. ":completed_job_jids", jid)

  -- TODO: check this at the top of this function
  if moved ~= 1 then
    error("JobNotPending: " .. jid)
  end

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

local PlinesAPI = {}

function PlinesAPI.expire_job_batch(
  now, pipeline_name, id, data_ttl_in_milliseconds
)
  return Plines.job_batch(pipeline_name, id):expire(data_ttl_in_milliseconds)
end

function PlinesAPI.complete_job(
  now, pipeline_name, id, jid, worker, data_ttl_in_milliseconds, now_iso8601
)
  return Plines.job_batch(pipeline_name, id):complete_job(
    jid, data_ttl_in_milliseconds, worker, now, now_iso8601
  )
end

-- Dispatch code. This must go last in the script.
if #KEYS > 0 then erorr('No Keys should be provided') end

local command_name = assert(table.remove(ARGV, 1), 'Must provide a command')
local command      = assert(
    PlinesAPI[command_name], 'Unknown command ' .. command_name)

local now          = tonumber(table.remove(ARGV, 1))
local now          = assert(
    now, 'Arg "now" missing or not a number: ' .. (now or 'nil'))

return command(now, unpack(ARGV))

