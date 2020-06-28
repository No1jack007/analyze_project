#!/bin/bash

SPARK_HOME='/opt/software/spark-2.4.1-bin-hadoop2.7'
JOB_LOCATION='/opt/application/analyze/analyze_project'
DATA_LOCATION='/opt/data/mysql/output-data'
MASTER='spark://hadoop-1:7077'
DB_PROPERTIES_LOCATION='/opt/application/analyze/analyze_project'
DEPART_ID='1'

${SPARK_HOME}/bin/spark-submit \
--class com.analyze.service.vehicle.VehicleAnalyze \
--master ${MASTER} \
--deploy-mode client \
--driver-memory 2g \
--total-executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/sys_veh_produce \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

${SPARK_HOME}/bin/spark-submit \
--class com.analyze.service.sale.SaleAnalyze \
--master ${MASTER} \
--deploy-mode client \
--driver-memory 2g \
--total-executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/sys_veh_sale \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

${SPARK_HOME}/bin/spark-submit \
--class com.analyze.service.repair.RepairAnalyze \
--master ${MASTER} \
--deploy-mode client \
--driver-memory 2g \
--total-executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/after_sale_repair_record \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

${SPARK_HOME}/bin/spark-submit \
--class com.analyze.service.retire.RetireAnalyze \
--master ${MASTER} \
--deploy-mode client \
--driver-memory 2g \
--total-executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/retired_battery_record \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}