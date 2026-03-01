#!/bin/bash
# Generate placeholder PNG icons using ImageMagick (or use your own 16/48/128px PNGs)
# Run once: bash icons/generate-icons.sh

for size in 16 48 128; do
  convert -size "${size}x${size}" xc:'#2383e2' \
    -fill white -gravity Center \
    -pointsize $((size / 2)) -annotate 0 "N" \
    "icons/icon${size}.png"
done
echo "Icons generated."
