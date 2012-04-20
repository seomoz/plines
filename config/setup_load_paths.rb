begin
  # use `bundle install --standalone' to get this...
  require_relative '../bundle/bundler/setup'
rescue LoadError
  # fall back to regular bundler if the person hasn't bundled standalone
  require 'bundler'
  Bundler.setup
end

