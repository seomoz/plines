if RUBY_ENGINE == 'ruby'
  require 'simplecov'

  SimpleCov.start do
    add_filter "/spec"
    add_filter "/bundle"
    add_filter "/config/setup_load_paths"
  end

  original_process = Process.pid

  SimpleCov.at_exit do
    if Process.pid == original_process
      file = File.join(SimpleCov.coverage_path, 'coverage_percent.txt')
      File.open(file, 'w') do |f|
        f.write SimpleCov.result.covered_percent
      end
      SimpleCov.result.format!
    end
  end
end

