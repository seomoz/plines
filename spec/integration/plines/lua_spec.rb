require 'spec_helper'
require 'plines/lua'

module Plines
  describe Lua, :redis do
    let(:lua) { Plines::Lua.new(redis) }

    it 'aldkfjdsakf' do
      lua.complete_job
    end
  end
end

