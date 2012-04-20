require_relative '../config/setup_load_paths'
require 'rspec/fire'

RSpec.configure do |config|
  config.treat_symbols_as_metadata_keys_with_true_values = true
  config.run_all_when_everything_filtered = true
  config.filter_run :f
  config.include RSpec::Fire
end
