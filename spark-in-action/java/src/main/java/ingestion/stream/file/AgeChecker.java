package ingestion.stream.file;

import org.apache.spark.sql.ForeachWriter;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgeChecker extends ForeachWriter<Row> {
    private static final long serianVersionUID = 8383715100587612498L;
    private static Logger log = LoggerFactory.getLogger(AgeChecker.class);
    private int streamId = 0;

    public AgeChecker(int streamId) {
        this.streamId = streamId;
    }

    // Implement method when you close your writer
    @Override
    public void close(Throwable arg0) {}

    // Implement method when you open your writer
    @Override
    public boolean open(long arg0, long arg1) {
        return true;
    }

    @Override
    public void process(Row arg0) {  // Method to process a row
        if (arg0.length() != 5) {  // Simple quality check to ensure that the record has five columns
            return;
        }
        int age = arg0.getInt(3);
        if (age < 13) {
            log.debug("On stream #{}: {} is a kid, they are {} yrs old.",
                    streamId,
                    arg0.getString(0),
                    age);
        } else if (age > 12 && age < 20) {
            log.debug("On stream #{}: {} is a teen, they are {} yrs old.",
                    streamId,
                    arg0.getString(0),
                    age);
        } else if (age > 64) {
            log.debug("On stream #{}: {} is a senior, they are {} yrs old.",
                    streamId,
                    arg0.getString(0),
                    age);
        }
    }
}
