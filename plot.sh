#!/bin/bash

plot_args=""
separator=""
count=1
for site in $* ; do
    title=`echo "$site" | sed -e 's/[^_]*_\([^_]\+\)_.*$/\1/'` 
    plot_args="${plot_args}${separator}'$site' using 1:2 title '$title' linecolor ${count}"
    separator=", "
    count=$((count+1))
done
#echo "$plot_args"

cat <<EOF | gnuplot
set datafile separator ","
set terminal png size 1000,800
set xdata time
set timefmt "%s"
set format x "%H:%M\n%d/%m"
set output 'plot.png'
plot $plot_args
EOF
