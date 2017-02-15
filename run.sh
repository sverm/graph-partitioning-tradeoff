#!/bin/bash

$SPARK_HOME/bin/spark-submit \
--class GraphPartitioningTradeoff \
--master spark://192.17.176.173:7077 \
--total-executor-cores=128 --driver-memory 10g --executor-memory 48g --executor-cores 16 \
--conf spark.default.parallelism=128 \
--conf spark.ui.showConsoleProgress=false \
target/scala-2.11/graph-partitioning-tradeoff_2.11-0.1-SNAPSHOT.jar $@
