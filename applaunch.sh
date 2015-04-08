#!/bin/bash

/home/twqc64/google-cloud-sdk/bin/bq -q --format=csv query --max_rows=1 --allow_large_results --destination_table="motorola.com:datasystems:mac.applaunch" --replace "`cat /home/twqc64/sandbox/demo/applaunch.bq`"

/home/twqc64/google-cloud-sdk/bin/bq extract --noprint_header motorola.com:datasystems:mac.applaunch "gs://guru-examples/data/applaunch/1-*.csv,gs://guru-examples/data/applaunch/2-*.csv,gs://guru-examples/data/applaunch/3-*.csv,gs://guru-examples/data/applaunch/4-*.csv"
