#!/bin/bash

# Define the input and output
INPUT="flightlist.json"
OUTPUT="flightlist.csv"

jq -r '
  ["flight","beacons","day","balloonsize","liftfactor","h2fill",
   "parachute.description","parachute.size","weights.client",
   "weights.eoss","weights.parachute","weights.neckload",
   "weights.balloon","weights.gross","weights.necklift"],
  (if type == "array" then .[] else . end | [
    .flight,
    (.beacons | tostring),
    .day,
    .balloonsize,
    .liftfactor,
    .h2fill,
    .parachute.description,
    .parachute.size,
    .weights.client,
    .weights.eoss,
    .weights.parachute,
    .weights.neckload,
    .weights.balloon,
    .weights.gross,
    .weights.necklift
  ]) | @csv' "$INPUT" > "$OUTPUT"

echo "Generated $OUTPUT with $(wc -l < $OUTPUT) rows."

