local PlinesAPI = {}

function PlinesAPI.expire_job_batch(
  pipeline_name, id, data_ttl_in_milliseconds
)
  return Plines.job_batch(pipeline_name, id):expire(data_ttl_in_milliseconds)
end

function PlinesAPI.complete_job(
  pipeline_name, id, jid, worker, data_ttl_in_milliseconds, now_iso8601
)
  return Plines.job_batch(pipeline_name, id):complete_job(
    jid, data_ttl_in_milliseconds, worker, now_iso8601
  )
end

function PlinesAPI.delete(pipeline_name, id)
  return Plines.job_batch(pipeline_name, id):delete()
end

function PlinesAPI.is_job_batch_awaiting_external_dependency(pipeline_name, id, dependency_name)
  return Plines.job_batch(pipeline_name, id):is_awaiting_external_dependency(dependency_name)
end

-- Dispatch code. This must go last in the script.
if #KEYS > 0 then error('No Keys should be provided') end

local command_name = assert(table.remove(ARGV, 1), 'Must provide a command')
local command      = assert(
    PlinesAPI[command_name], 'Unknown command ' .. command_name)

return command(unpack(ARGV))

