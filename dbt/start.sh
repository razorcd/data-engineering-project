#!/bin/bash

let i=0
while true
do
   let i++
   echo "Starting DBT run $i"
   dbt run --profiles-dir /dbt --full-refresh
   echo "Run $i complete. Waiting 15min for next run."
   echo $(date)
   sleep 15m 
done