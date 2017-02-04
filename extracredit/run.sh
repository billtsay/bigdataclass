#!/bin/bash

spark-1.6.1/bin/spark-submit --class $1 \
--master local[*] assignment.jar $2 $3 $4 $5

