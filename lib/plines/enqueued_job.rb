require 'redis/objects'

module Plines
  # Once a Plines::Job has been enqueued as a Qless job into redis,
  # an EnqueuedJob is used to represent and hold the additional state
  # that Plines needs to track about the job.
  class EnqueuedJob < Struct.new(:jid)
    include Redis::Objects

    def initialize(jid, &block)
      super(jid)
      instance_eval(&block) if block
    end

    def self.create(jid, *external_dependencies)
      new(jid) do
        external_dependencies.each do |dep|
          pending_ext_deps << dep
        end
      end
    end

    def pending_external_dependencies
      pending_ext_deps.map(&:to_sym)
    end

    def resolved_external_dependencies
      resolved_ext_deps.map(&:to_sym)
    end

    def resolve_external_dependency(name)
      moved_count, remaining = redis.multi do
        pending_ext_deps.move(name, resolved_ext_deps)
        pending_ext_deps.length
      end

      if moved_count == 0
        raise ArgumentError, "EnqueuedJob #{jid} does not have external " +
                             "dependency #{name.inspect}"
      end

      yield if remaining.zero?
    end

  private

    alias id jid # id is needed by Redis::Objects
    set :pending_ext_deps
    set :resolved_ext_deps
  end
end

