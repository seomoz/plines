require 'qless'

module Plines
  # Provides access to Plines' lua scripts.
  class Lua
    def initialize(redis)
      @redis = redis
    end

    def expire_job_batch(job_batch)
      call :expire_job_batch,
        job_batch.pipeline.name,
        job_batch.id,
        job_batch.pipeline.configuration.data_ttl_in_milliseconds
    end

    def complete_job(job_batch, qless_job)
      call :complete_job,
        job_batch.pipeline.name,
        job_batch.id,
        qless_job.jid,
        job_batch.pipeline.configuration.data_ttl_in_milliseconds,
        Time.now.getutc.iso8601
    rescue Qless::LuaScriptError => e
      raise unless e.message.start_with?('JobNotPending')

      raise JobBatch::JobNotPendingError, "Jid #{qless_job.jid} cannot be " +
        "marked as complete for job batch #{job_batch.id} since it is not pending"
    end

  private

    def call(command, *args)
      script.call(command, Time.now.to_i, *args)
    end

    def self.lua_table_from(klass)
      items = klass.declared_redis_object_names.map { |i|
        %Q|"#{i}"|
      }.join(", ")

      "{ #{items} }"
    end

    def self.base_script_contents
      File.read(File.expand_path("../lua/plines.lua", __FILE__))
    end

    def self.lua_constants
      <<-EOS
        local plines_job_batch_sub_keys = #{lua_table_from JobBatch}
        local plines_enqueued_job_sub_keys = #{lua_table_from EnqueuedJob}
      EOS
    end

    def self.script_contents
      @script_contents ||= lua_constants + base_script_contents
    end

    def script
      @script ||= Qless::LuaPlugin.new("plines", @redis,
                                       self.class.script_contents)
    end
  end
end

