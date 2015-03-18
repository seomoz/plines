require 'logger'
require 'plines/indifferent_hash'

module Plines
  # Stores global Plines configuration.
  class Configuration
    # Raised when there is a configuration error.
    class Error < StandardError; end

    TWO_MONTHS_IN_SECONDS = 2 * 30 * 24 * 60 * 60

    def initialize
      qless_client   { raise Error, "qless_client has not been configured" }
      batch_list_key { raise Error, "batch_list_key has not been configured" }

      qless_job_options { |job| {} }
      self.data_ttl_in_seconds = TWO_MONTHS_IN_SECONDS
      @callbacks = Hash.new { |h, k| h[k] = [] }
      @logger = Logger.new($stdout)
    end

    def qless_client(&block)
      @qless_client_block = block
    end

    def qless_client_for(key)
      @qless_client_block[key]
    end

    def batch_list_key(&block)
      @batch_list_key_block = block
    end

    def batch_list_key_for(batch_data)
      @batch_list_key_block[batch_data]
    end

    attr_reader :logger
    def logger=(value)
      @logger = value
    end

    attr_accessor :data_ttl_in_seconds
    def data_ttl_in_milliseconds
      (data_ttl_in_seconds * 1000).to_i
    end

    attr_reader :qless_job_options_block
    def qless_job_options(&block)
      @qless_job_options_block = block
    end

    def after_job_batch_cancellation(&block)
      @callbacks[:after_job_batch_cancellation] << block
    end

    def notify(callback_type, *args)
      @callbacks[callback_type].each do |callback|
        callback.call(*args)
      end
    end

    # Indifferent hashes are convenient but slow things down considerably.
    # See `benchmarks/using_indifferent_hash`.
    attr_accessor :expose_indifferent_hashes

    def exposed_hash_from(hash)
      if expose_indifferent_hashes
        Plines::IndifferentHash.from(hash)
      else
        hash
      end
    end
  end
end

