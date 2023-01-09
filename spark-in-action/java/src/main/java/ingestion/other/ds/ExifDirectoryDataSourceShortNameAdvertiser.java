package ingestion.other.ds;


import org.apache.spark.sql.sources.DataSourceRegister;

public class ExifDirectoryDataSourceShortNameAdvertiser
    extends  ExifDirectoryDataSource  // Customized data source code
    implements DataSourceRegister {

    /**
     * Implements ths shortName() method to return the short name that wish
     * to use for custom data source
     */
    @Override
    public String shortName() {
        return "exif";
    }
}
