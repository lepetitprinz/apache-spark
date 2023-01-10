package ingestion.stream.utils.lib;

public class RecordGeneratorException extends Exception {
    private static final long serialVersionUID = 4046912590125990484L;

    public RecordGeneratorException(String message, Exception e) {
        super(message, e);
    }
}
