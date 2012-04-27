module Plines
  # Stores global Plines configuration.
  class Configuration
    # Raised when there is a configuration error.
    class Error < StandardError; end

    def initialize
      batch_list_key { raise Error, "batch_list_key has not been configured" }
    end

    def batch_list_key(&block)
      @batch_list_key_block = block
    end

    def batch_list_key_for(batch_data)
      @batch_list_key_block[batch_data]
    end
  end
end

