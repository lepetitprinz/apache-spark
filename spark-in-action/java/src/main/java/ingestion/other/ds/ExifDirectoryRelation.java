package ingestion.other.ds;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import ingestion.other.extlib.ExifUtils;
import ingestion.other.extlib.PhotoMetadata;
import ingestion.other.extlib.RecursiveExtensionFilteredLister;
import ingestion.other.utils.Schema;
import ingestion.other.utils.SparkBeanUtils;

public class ExifDirectoryRelation
        extends BaseRelation
        implements Serializable, TableScan {
    private static final long serialVersionUID = 4598175080399877334L;
    private static transient Logger log =
            LoggerFactory.getLogger(ExifDirectoryRelation.class);
    private SQLContext sqlContext;  // need the application SQL context and must implement the getter
    private Schema schema = null;  // Caches the schema to avoid recalculation
    private RecursiveExtensionFilteredLister photoLister;

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    public void setSqlContext(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    /**
     * Build and returns the schema as a StructType.
     */
    @Override
    public StructType schema() {
        if (schema == null) {
            schema = SparkBeanUtils.getSchemaFromBean(PhotoMetadata.class);
        }
        return schema.getSparkSchema();
    }

    @Override
    public RDD<Row> buildScan() {
        log.debug("-> buildScan()");
        schema();

        List<PhotoMetadata> table = collectData();  // to build a list with all metadata

        @SuppressWarnings("resource")
        JavaSparkContext sparkContext =  // Extracts the Spark context from the SQL context
                new JavaSparkContext(sqlContext.sparkContext());
        JavaRDD<Row> rowRDD = sparkContext.parallelize(table)
                .map(photo -> SparkBeanUtils.getRowFromBean(schema, photo));

        return rowRDD.rdd();
    }

    /**
     * Interface with the real world: the "plumbing" between Spark and
     * existing data, in our case the classes in charge of reading the
     * information from the photos.
     *
     * The list of photos will be "mapped" and transformed into a Row.
     */
    private List<PhotoMetadata> collectData() {
        // Builds a list of all the files by using the options
        List<File> photosToProcess = this.photoLister.getFiles();
        List<PhotoMetadata> list = new ArrayList<>();

        PhotoMetadata photo;
        for (File photoToProcess : photosToProcess) {
            photo = ExifUtils.processFromFilename(  // Extract the metadata from the file
                    photoToProcess.getAbsolutePath());
            list.add(photo);
        }
        return list;
    }

    public void setPhotoLister(
            RecursiveExtensionFilteredLister photoLister) {
        this.photoLister = photoLister;
    }
}