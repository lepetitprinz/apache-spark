/opt/spark/bin/spark-submit --master spark://spark-master:7077 \
--class ingestion.ComplexCsvToDataframeApp \
/opt/spark-apps/standalone-cluster-1.0-SNAPSHOT.jar