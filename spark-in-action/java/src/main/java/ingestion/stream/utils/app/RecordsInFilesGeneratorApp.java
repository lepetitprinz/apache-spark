package ingestion.stream.utils.app;

import ingestion.stream.utils.lib.FieldType;
import ingestion.stream.utils.lib.RecordGeneratorUtils;
import ingestion.stream.utils.lib.RecordStructure;
import ingestion.stream.utils.lib.RecordWriterUtils;
import ingestion.stream.utils.lib.StreamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class RecordsInFilesGeneratorApp {
    private static Logger log = LoggerFactory
            .getLogger(RecordsInFilesGeneratorApp.class);

    public int streamDuration = 60;  // Streaming duration in seconds
    public int batchSize = 10;  // Maximum number of records send at the same time
     //Wait time between two batches of records, in seconds,  with an element of variability
    public int waitTime = 5;

    public static void main(String[] args) {
        String outputDirectory = StreamingUtils.getInputDirectory();
        if (args.length == 2
            && args[0].compareToIgnoreCase("--output-directory") == 0) {
            outputDirectory = args[1];
            File dir = new File(outputDirectory);
            dir.mkdirs();
        }

        // Structure of the record
        RecordStructure rs = new RecordStructure("contact")
                .add("fname", FieldType.FIRST_NAME)
                .add("mname", FieldType.FIRST_NAME)
                .add("lname", FieldType.LAST_NAME)
                .add("age", FieldType.AGE)
                .add("ssn", FieldType.SSN);

        RecordsInFilesGeneratorApp app = new RecordsInFilesGeneratorApp();
        app.start(rs, outputDirectory);
    }

    private void start(RecordStructure rs, String outputDirectory) {
        log.debug("-> start (..., {})", outputDirectory);
        long start = System.currentTimeMillis();
        while (start + streamDuration * 1000L > System.currentTimeMillis()) {
            // Will generate up to batch size records in the file
            int maxRecord = RecordGeneratorUtils.getRandomInt(batchSize) + 1;

            // Writes the record in a file
            RecordWriterUtils.write(
                    rs.getRecordName() + "_" + System.currentTimeMillis() + ".txt",
                    rs.getRecords(maxRecord, false),
                    outputDirectory);
            try {
                Thread.sleep(RecordGeneratorUtils.getRandomInt(waitTime * 1000)
                + waitTime * 1000L / 2);  // Waits a random time
            } catch (InterruptedException ignored) {}
        }
    }
}
