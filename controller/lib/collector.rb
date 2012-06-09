#!/usr/bin/env ruby

require 'socket'
require 'thread'
require 'timeout'
require 'io/wait'
require 'oj' #json parser

# TODO: refactor class vars => instance vars
class Collector
  attr_accessor :planner
  def initialize(planner)
    self.planner = planner
  end
  
  # launches measurement client listener & measurement pulling threads
  def exec!
    listener = Collector.listen!
    Thread.new do
      loop do
        sleep(15)
        planner.next_histogram(Collector.next_workload_histogram!)
      end
    end
  end
  
  
  # returns listen loop thread
  # responsible for accepting new measurement client connections
  # and adding them to the pool of measurers to pull results from
  def self.listen!
    Thread.new do
      loop do
        # block until new connection opens
        new_measurer_socket = self.server.accept
        new_measurer_socket.set_encoding(Encoding::UTF_8)

        # safely add socket to list of sockets w/ mutex
        measurer_sockets_lock.synchronize do
          measurer_sockets.push new_measurer_socket
        end
        print_to_terminal "new measurer registered\n"
      end
    end
  end


  private

  # send pull and get data from the socket
  # convert the intermediary format to a native data structure
  # merge the individual measurements into one histogram
  # also logs result asynchronously (non-blocking) for graphing
  def self.next_workload_histogram!
    measurements = pull_measurements!
    
    workload_histogram = merge_histograms(
      deserialise_json_queue(measurements[:workload_histograms])
    )
    
    log(workload_histogram, measurements[:timing_stats])
    workload_histogram
  end
  
  # query the measurement clients for their current period measurements
  # returns an array of JSON histograms
  def self.pull_measurements!
    # fix the clients to use in this measurement
    clients = []
    self.measurer_sockets_lock.synchronize do
      clients = measurer_sockets.dup
    end
    
    # threadsafe result stores
    workload_histograms = Queue.new
    timing_stats = Queue.new
    
    # pull from each measurement client in parallel: fork/join
    histogram_pull_threads = clients.map do |sock|
      Thread.start(sock) do |sock|
        begin
          Timeout::timeout(30) do # seconds
            # print "send pull request to the java client"
            sock.putc 42

            # io calls block until we receive response
            # The first 2 bytes tell us how long the string is; discard
            # the transmitted data is linebreak delimited
            
            # read the histogram
            sock.read(2)
            req_histogram = sock.readline
            workload_histograms.enq(req_histogram)

            # now read the stats
            sock.read(2)  # The first 2 bytes tell us how long the string is; discard
            timing_stats.enq(sock.readline)
            
            # anything left in the socket is a coding error
            print_to_terminal "garbage in socket: #{sock.getc}\n  " while sock.ready?
          end
        rescue Exception => e
          print_to_terminal "removing socket: #{e}\n"
          measurer_sockets_lock.synchronize do
            measurer_sockets.delete(sock)
            # print_to_terminal"remaining sockets: #{measurer_sockets}\n"
          end
          sock.close
        end
      end
    end

    print_to_terminal('blocking waiting on threads ' + histogram_pull_threads.inspect)
    # wait for each pull thread to complete
    histogram_pull_threads.each(&:join)

    {:workload_histograms => workload_histograms, :timing_stats => timing_stats}
  end
  
  # workload_histogram is a hash
  # timing_stats is a queue
  # records each entry on 4 lines:
  # 1: timestamp
  # 2: workload histogram as json
  # 3: timing measurements array as json
  # 4: empty line
  def self.log(workload_histogram, timing_stats_queue)
    timing_stats = deserialise_json_queue(timing_stats_queue)
    
    # don't print when there are no measurements reported
    return if (timing_stats.empty?) && (workload_histogram.empty?)
    
    # queue_to_array(timing_stats_queue)
    # timing_stats.map! {|hash| Oj.dump(hash)}
    #sync file access
    log_lock.synchronize do
      # open for append-only writing; creates if not found
      File.open('measurements.log', 'a') do |f|
        f.puts Time.now.strftime('%H:%M:%S')
        f.puts Oj.dump(workload_histogram)
        f.puts Oj.dump(timing_stats)
        f.puts ''
      end
    end
  end
  
  def self.queue_to_array(queue)
    array = []
    array.push(queue.deq) until queue.empty?
    array
  end
  
  # given a Queue of json data
  # returns an array of nested ruby data structures
  def self.deserialise_json_queue(json_queue)
    queue_to_array(json_queue).map {|json| Oj.load(json) }
  end

  # Input: an array of histogram hashes
  # Output: a single histogram hash, with request counts summed
  # recall histogram comprises nested hashes, e.g. 
  # {"0"=>{"PUT"=>338, "GET"=>1260, "DELETE"=>0}, "1"=>{"PUT"=>309, ...
  def self.merge_histograms(histograms)
    histogram = Hash.new do |hash, key|
      hash[key] = {'GET' => 0, 'PUT' => 0, 'DELETE' => 0}
    end
    histograms.reduce(histogram) do |accumulator, next_partition_hist|
      # merge! adopts a single set value, and uses the block to resolve conflicts
      # at this scope we have
      # {"0"=>{"PUT"=>338, "GET"=>1260, "DELETE"=>0}, "1"=>{"PUT"=>309, ...
      accumulator.merge!(next_partition_hist) do |partition, accum_reqs_hash, next_reqs_hash|
        # at this scope we have
        # {"PUT"=>338, "GET"=>1260, "DELETE"=>0}
        accum_reqs_hash.merge!(next_reqs_hash) do |request, accum_count, next_measured_count|
          accum_count + next_measured_count
        end
      end
    end
  end

  def self.print_to_terminal(msg)
    print "#{Time.now.strftime('%H:%M:%S')}: #{msg}\n"
  end


  # init code
  class << self
    attr_accessor :server
    attr_accessor :measurer_sockets
    attr_accessor :measurer_sockets_lock
    attr_accessor :log_lock
  end
  self.server = TCPServer.new 27960
  self.measurer_sockets = []
  self.measurer_sockets_lock = Mutex.new
  self.log_lock = Mutex.new
end

# listener = Collector.listen!
# loop do
#   sleep(15)
#   puts "result #{Collector.next_workload_histogram!}"
# end
# 3.times do
#   m = TCPSocket.new('localhost', 27960)
#   Thread.start(m) do |sock|
#     puts 'waiting for pull'
#     puts sock.readline
#     sock.puts('{measured results}')
#   end
# end
# 
# sleep(2)
# 
# Collector.pull_measurements!
