#!/bin/bash

SPARK_HOME='/opt/software/spark-2.4.1-bin-hadoop2.7'
JOB_LOCATION='/opt/application/analyze/analyze_project'
DATA_LOCATION='/opt/data/mysql/output-data'
MASTER='spark://hadoop-1:7077'
DB_PROPERTIES_LOCATION='/opt/application/analyze/analyze_project'
DEPART_ID='1'
DRIVER_MODE='cluster'

${SPARK_HOME}/bin/spark-submit \
--master ${MASTER} \
--deploy-mode ${DRIVER_MODE} \
--class com.analyze.service.produce.ProduceAnalyze \
--driver-memory 2g \
--executor-memory 4g \
--total-executor-cores 4 \
--executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/sys_veh_produce \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

${SPARK_HOME}/bin/spark-submit \
--master ${MASTER} \
--deploy-mode ${DRIVER_MODE} \
--class com.analyze.service.sale.SaleAnalyze \
--driver-memory 2g \
--executor-memory 4g \
--total-executor-cores 4 \
--executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/sys_veh_sale \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

${SPARK_HOME}/bin/spark-submit \
--master ${MASTER} \
--deploy-mode ${DRIVER_MODE} \
--class com.analyze.service.repair.RepairAnalyze \
--driver-memory 2g \
--executor-memory 4g \
--total-executor-cores 4 \
--executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/after_sale_repair_record \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

${SPARK_HOME}/bin/spark-submit \
--master ${MASTER} \
--deploy-mode ${DRIVER_MODE} \
--class com.analyze.service.retire.RetireAnalyze \
--driver-memory 2g \
--executor-memory 4g \
--total-executor-cores 4 \
--executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/retired_battery_record \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

${SPARK_HOME}/bin/spark-submit \
--master ${MASTER} \
--deploy-mode ${DRIVER_MODE} \
--class com.analyze.service.produce.ProduceSaleAnalyze \
--driver-memory 2g \
--executor-memory 4g \
--total-executor-cores 4 \
--executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/sys_veh_produce \
${DATA_LOCATION}/sys_veh_sale \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

${SPARK_HOME}/bin/spark-submit \
--master ${MASTER} \
--deploy-mode ${DRIVER_MODE} \
--class com.analyze.service.sale.VehicleAgeAnalyze \
--driver-memory 2g \
--executor-memory 4g \
--total-executor-cores 4 \
--executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/sys_veh_sale \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

${SPARK_HOME}/bin/spark-submit \
--master ${MASTER} \
--deploy-mode ${DRIVER_MODE} \
--class com.analyze.service.sale.UserPortraitAnalyze \
--driver-memory 2g \
--executor-memory 4g \
--total-executor-cores 4 \
--executor-cores 4 \
${JOB_LOCATION}/analyze-project-jar-with-dependencies.jar \
${MASTER} \
${DATA_LOCATION}/sys_veh_sale \
${DB_PROPERTIES_LOCATION}/db.properties \
${DEPART_ID}

