#!/usr/bin/env ruby


# the following histogram plot works, but the x-axis is ugly, as it does not normalise time to e.g. 0,30,60, etc, rather it prints each sample's time exactly as the labels
# see http://psy.swansea.ac.uk/staff/Carter/gnuplot/gnuplot_time_histograms.htm
# partial cmd:
# results.dat" u 2:xtic(1) t "Response time" axes x1y2 w linesp, "" u 3:xtic(1) t "p0" w histograms, "" u 4 t "p1" w histograms, "" u 5 t "p2" w histograms
# puts <<HEREDOC
# set style fill pattern
# set style histogram rowstacked
# set key top left reverse Left
# set ylabel "GET/s"
# set y2label "Response time (worst-sampled 99th%) ms"
# set y2tics
# set ytics nomirror
# set xtics rotate by 270
# plot [][0:500000][][0:10] "results.dat" u 2:xtic(1) t "Response time" axes x1y2 w linesp, #{(1..11).map{|n| %Q("" u #{n+3}:xtic(1) t "p#{n}" w histograms)}.join(', ')}
# save "graph.gp"
# set terminal pdf
# set output "graph.pdf"
# replot
# HEREDOC
# 

def vars_for_stripe(stripe)
  vars = (2..stripe).map {|n| "$#{n}"}
  vars.join('+')
end

# set style fill pattern
# set key top left reverse Left
puts <<HEREDOC
set key outside
set xlabel "Time (s)"
set ylabel "GET/s"
set y2label "Response time (worst-sampled 99th%) ms"
set y2tics
set ytics nomirror
set logscale y2 2
plot [0:*][0:*][][1:16] "results.dat" u 1:2 t "RT" axes x1y2 w linesp, #{(2..13).to_a.reverse!.map{|col| %Q("" u 1:(#{vars_for_stripe(col)}) axes x1y1 t "p#{col-2}" w boxes)}.join(', ')}

save "graph.gp"
set terminal pdf
set output "graph.pdf"
replot
HEREDOC
