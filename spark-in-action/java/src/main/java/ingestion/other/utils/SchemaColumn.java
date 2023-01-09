package ingestion.other.utils;

import java.io.Serializable;

public class SchemaColumn implements Serializable {
    private static final long serialVersionUID = 9113201899451270469L;
    private String methodName;
    private String columnName;

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String method) {
        this.methodName = method;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
