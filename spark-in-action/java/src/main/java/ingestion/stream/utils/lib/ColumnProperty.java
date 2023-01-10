package ingestion.stream.utils.lib;

public class ColumnProperty {

    private FieldType recordType;
    private String option;

    public ColumnProperty(FieldType recordType, String option) {
        this.recordType = recordType;
        this.option = option;
    }

    public FieldType getRecordType() {
        return recordType;
    }

    public void setRecordType(FieldType recordType) {
        this.recordType = recordType;
    }

    public String getOption() {
        return option;
    }

    public void setOption() {
        this.option = option;
    }
}
