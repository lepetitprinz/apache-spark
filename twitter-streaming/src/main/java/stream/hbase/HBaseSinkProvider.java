package stream.hbase;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.streaming.Sink;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.StreamSinkProvider;
import org.apache.spark.sql.streaming.OutputMode;
import scala.collection.immutable.Map;
import scala.collection.immutable.Seq;

public class HBaseSinkProvider implements DataSourceRegister, StreamSinkProvider {
    public Sink createSink(SQLContext sqlContext, Map<String, String> parameters,
                           Seq<String> partitionColumns, OutputMode outputMode) {
        HBaseSink hBaseSink = new HBaseSink(parameters);
        return hBaseSink;
    }

    public String shortName() {
        return "hbase";
    }
}
