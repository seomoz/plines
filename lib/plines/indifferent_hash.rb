require 'delegate'

module Plines
  # Provides a hash that can be accessed by symbol or key.
  # This is useful because a plines job batch data hash is commonly
  # provided with symbol keys, but after round-tripping through
  # JSON it is converted to strings. We can't safely convert all
  # strings to symbols (as symbols are never GC'd) so instead we
  # use this for the data hash.
  class IndifferentHash < DelegateClass(Hash)
    NotAHashError = Class.new(TypeError)

    private_class_method :new

    def self.from(original)
      unless original.is_a?(Hash) || original.is_a?(IndifferentHash)
        raise NotAHashError, "Expected a hash, got #{original.inspect}"
      end

      indif = Hash.new { |hash, key| hash[key.to_s] if Symbol === key }

      original.each_with_object(indif) do |(key, value), hash|
        hash[key.to_s] = indifferent(value)
      end

      new(indif)
    end

    def self.indifferent(object)
      case object
      when Hash  then from(object)
      when Array then object.map { |o| indifferent(o) }
      else object
      end
    end

    def fetch(key)
      super
    rescue KeyError => error
      raise unless Symbol === key && has_key?(key.to_s)
      super(key.to_s)
    end
  end
end

