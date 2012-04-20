require "plines/version"
require 'plines/step'
require 'qless'

module Plines
  extend self
  attr_writer :qless

  def qless
    @qless ||= Qless::Client.new
  end

  def default_queue
    @default_queue ||= qless.queue("plines")
  end

  def start_processing(data = {})
    Plines::Step.all.each do |klass|
      default_queue.put(klass, {})
    end
  end
end

