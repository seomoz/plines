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

function PlinesEnqueuedJob:expire(now, data_ttl_in_milliseconds)
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

function PlinesJobBatch:expire(now, data_ttl_in_milliseconds)
  for _, sub_key in ipairs(plines_job_batch_sub_keys) do
    redis.call('pexpire', self.key .. ":" .. sub_key, data_ttl_in_milliseconds)
  end

  for _, jid in ipairs(self:jids()) do
    local job = Plines.enqueued_job(self.pipeline_name, jid)
    job:expire(now, data_ttl_in_milliseconds)

    for _, dep in ipairs(job:external_dependencies()) do
      redis.call('pexpire', self.key .. ":ext_deps:" .. dep, data_ttl_in_milliseconds)
      redis.call('pexpire', self.key .. ":timeout_job_jids:" .. dep, data_ttl_in_milliseconds)
    end
  end
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
  return Plines.job_batch(
    pipeline_name, id
  ):expire(now, data_ttl_in_milliseconds)
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

