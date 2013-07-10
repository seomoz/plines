require 'delegate'

module Plines
  # Provides a hash that can be accessed by symbol or string keys.
  # This is useful because a plines job batch data hash is commonly
  # provided with symbol keys, but after round-tripping through
  # JSON it is converted to strings. We can't safely convert all
  # strings to symbols (as symbols are never GC'd) so instead we
  # use this for the data hash.
  class IndifferentHash < DelegateClass(Hash)
    NotAHashError = Class.new(TypeError)
    ConflictingEntriesError = Class.new(ArgumentError)

    private_class_method :new

    def self.from(original)
      unless original.is_a?(Hash) || original.is_a?(IndifferentHash)
        raise NotAHashError, "Expected a hash, got #{original.inspect}"
      end

      indif = Hash.new { |hash, key| hash[key.to_s] if Symbol === key }

      original.each do |key, value|
        key = key.to_s

        if indif.has_key?(key)
          raise ConflictingEntriesError,
            "Hash has conflicting entries for #{key}: #{original}"
        end

        indif[key] = indifferent(value)
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

    def fetch(key, *args)
      key = indifferent_key_from(key)
      super
    end

    def delete(key)
      key = indifferent_key_from(key)
      super
    end

    def merge(other)
      IndifferentHash.from super(IndifferentHash.from other)
    end

    def with_indifferent_access
      self
    end

  private

    def indifferent_key_from(key)
      if !has_key?(key) && Symbol === key && has_key?(key.to_s)
        key.to_s
      else
        key
      end
    end
  end
end

