require 'qless'

module Plines
  class Lua
    def initialize(redis)
      @redis = redis
    end

    def complete_job
      call :complete_job
    end

  private

    def call(command, *args)
      script.call(command, Time.now.to_i, *args)
    end

    def script
      @script ||= Script.new(@redis)
    end

    class Script < ::Qless::LuaScript
      def initialize(redis)
        super("plines_compiled", redis)
      end

    private

      def script_contents
        @script_contents ||= %w[ qless-lib.lua plines.lua ].map do |name|
          File.read(File.join(SCRIPT_ROOT, name))
        end.join("\n\n")
      end

      SCRIPT_ROOT = File.expand_path("../lua", __FILE__)
    end
  end
end

