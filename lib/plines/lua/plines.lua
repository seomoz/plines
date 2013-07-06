local PlinesAPI = {}

function PlinesAPI.complete_job(now, jid, job_batch_id, worker, queue, data, ...)
  Qless.job(jid):complete(now, worker, queue, data)
  error(redis.keys)
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

