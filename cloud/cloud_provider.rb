#!/usr/bin/env ruby

require 'sinatra/base'

# each request spawns an instance of the class
# so we store the data on the class (singleton) object
# this isn't threadsafe but we only have 1 controller so let's assume it's ok
class CloudProvider < Sinatra::Base
  class << self # open the class's object
    # add accessors and initialise the hostname arrays
    attr_accessor :in_use
    attr_accessor :available_nodes
  end
  self.in_use = []
  self.available_nodes = (1..5).map {|n| "Lakka-#{n}.it.kth.se"}
  
  # the rest of the class defines accepted url paths and responses
  
  # which nodes are currently in use
  get '/nodes' do
    settings.in_use.join(' ')
  end


  # returns the hostname (or ip) of a new node
  # e.g. POST /nodes/allocate
  post '/nodes/allocate' do
    node = settings.available_nodes.pop
    if node.nil? 
      507
    else
      settings.in_use << node
      node
    end
  end

  # e.g. POST /nodes/release/lakka-2.it.kth.se
  post '/nodes/release/:host' do
    raise 'not in use' unless settings.in_use.include? params[:host]
    settings.in_use.delete params[:host]
    settings.available_nodes.push params[:host]
    200 # HTTP 200 OK
  end

  # start the server if ruby file executed directly
  run! if app_file == $0
end

