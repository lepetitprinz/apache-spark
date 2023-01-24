package stream.hbase;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.datasources.hbase.HBaseTableCatalog;
import org.apache.spark.sql.execution.streaming.Sink;
import java.util.HashMap;
import scala.collection.immutable.Map;


public class HBaseSink implements Sink {
    Map options;

    HBaseSink(Map<String, String> options) {
        this.options = options;
    }

    private String hBaseCatalog = options.get("hbasecat").toString();

    @Override
    public void addBatch(long batchId, Dataset<Row> data) {
        Dataset<Row> df = data.sparkSession().createDataFrame(data.rdd(), data.schema());
        HashMap<String, String> option = new HashMap<>();
        option.put(HBaseTableCatalog.tableCatalog(), hBaseCatalog);
        option.put(HBaseTableCatalog.newTable(), "5");
        df
                .write()
                .options(option)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save();
    }
}
