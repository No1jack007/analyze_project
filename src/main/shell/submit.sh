#!/bin/bash

JOB_HOME=/opt/analyze/analyze_project
SPARK_HOME=/op/vehicle/

cd ${SPARK_HOME}

spark2-submit \
--class com.analyze.vehicle.VehicleAnalyze \
--executor-memory 4G \
--executor-cores 8 \
