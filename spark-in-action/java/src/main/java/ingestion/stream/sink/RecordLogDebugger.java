package ingestion.stream.sink;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordLogDebugger extends ForeachWriter<Row> {
    private static final long serialVersionUID = 4137020658417523102L;
    private static Logger log =
            LoggerFactory.getLogger(RecordLogDebugger.class);
    private static int count = 0;

    @Override
    public void close(Throwable arg0) {}

    @Override
    public boolean open(long arg0, long arg1) { return true; }

    @Override
    public void process(Row arg0) {
        count++;
        log.debug("Record #{} has {} column(s)", count, arg0.length());
        log.debug("First value: {}", arg0.get(0));
    }
}
