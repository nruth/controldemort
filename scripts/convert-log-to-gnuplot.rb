#!/usr/bin/env ruby

DISCARD_N_STARTUP_SAMPLES = 1

require 'oj' # json lib

def timestamp_to_seconds(timestamp)
  hours, minutes, seconds = /(\d{2}):(\d{2}):(\d{2})/.match(timestamp).to_a[1..3].map(&:to_i)
  "#{hours} #{minutes} #{seconds}"
  (hours*60*60) + (minutes*60) + seconds
end

File.open('results.dat', 'w') do |out|

  # each call to gets will read a line from stdin, or the filename argument
  # data is structured
  # timestamp
  # histogram
  # timing samples
  # 
  # note the trailing blank line
  counter = 0
  t_previous = 0
  while timestamp = gets
    timestamp.strip! # remove the linebreak
    workload_histogram = Oj.load(gets)
    timing_samples = Oj.load(gets)
    empty_line = gets

    # convert timestamps to integer seconds, for normalised x-axis
    t_seconds = timestamp_to_seconds(timestamp)
    start_time ||= t_seconds
    t_seconds -= start_time

    # discard warmup samples
    counter += 1
    next if counter <= DISCARD_N_STARTUP_SAMPLES

    partitions = workload_histogram.keys.sort!
    latency = timing_samples.reduce(0) do |prev, next_|
      [prev, next_['99th%'].to_f].max
    end

    # write gnuplot compatible columnar data
    # time latency part0 part1 part2 part3...
    out.print "#{t_seconds} #{latency}"
    partitions.each do |partition|
      # normalise throughput to per second
      ops_for_sample = workload_histogram[partition]['GET']
      throughput = ops_for_sample.to_f / (t_seconds - t_previous)
      out.print " #{throughput}"
    end
    out.puts # linebreak

    t_previous = t_seconds # advance for the next iteration
  end
end