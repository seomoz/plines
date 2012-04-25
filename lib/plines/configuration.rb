module Plines
  # Stores global Plines configuration.
  class Configuration
    # Raised when there is a configuration error.
    class Error < StandardError; end

    def initialize
      batch_group_key { raise Error, "batch_group_key has not been configured" }
    end

    def batch_group_key(&block)
      @batch_group_key_block = block
    end

    def batch_group_for(batch_data)
      @batch_group_key_block[batch_data]
    end
  end
end

