#!/bin/bash

if [ -n "$SPARK_OPTS" ]; then
  SPARK_OPTS="--executor-memory 4G"
fi

bin/spark-submit --class recognai.kg.builder.rdf.spark.pipeline.Main \
  --master $SPARK_MASTER_URL \
  $SPARK_OPTS \
  --deploy-mode client \
  /app/app.jar $CONFIG_URL
