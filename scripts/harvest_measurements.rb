#!/usr/bin/env ruby

target_throughput = ARGV[0].to_i

# 4 - scp measurement log from controller
system "scp emdc@lakka-6.it.kth.se:nick/measurements.log . && ssh emdc@lakka-6.it.kth.se 'rm nick/measurements.log'"

# 5 - store log in dir based on cluster size and target throughput
dir_name = "#{target_throughput}-gets"

result_path = "/Users/nruth/Dropbox/thesis-results/#{dir_name}"

system "mkdir -p #{result_path} && mv measurements.log #{result_path}/"

Dir.chdir result_path do
  # 6 - convert log to gnuplot format
  system "~/thesis-code/scripts/convert-log-to-gnuplot.rb measurements.log"

  # 7 plot
  system "~/thesis-code/scripts/gnuplot-hist-cmd.rb | gnuplot"
end

puts "check #{result_path} for output"
