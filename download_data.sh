#!/usr/bin/env bash

set -e

# This script downloads the data from
# https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

output_dir="data/"

if [ ! -d "${output_dir}" ]; then
  mkdir -v "${output_dir}"
else
  echo \
    "Error: \"${output_dir}\" dir exists. Remove it before running this script." && \
    exit 1
fi

resource_name="https://s3.amazonaws.com/nyc-tlc/trip+data"

months="$(seq -f "%02g" 1 12)"

for month in ${months}; do
  file="yellow_tripdata_2020-${month}.csv"
  # save file with underscore separators as hyphen isn't very portable:
  wget -O "${output_dir}${file//-/_}" "${resource_name}/${file}"
done

echo "Success! All data downloaded in the ${output_dir}"
