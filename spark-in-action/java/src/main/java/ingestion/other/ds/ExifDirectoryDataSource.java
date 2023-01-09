package ingestion.other.ds;

// Static method to convert Scala map to Java map
import static scala.collection.JavaConverters.mapAsJavaMapConverter;

import ingestion.other.extlib.RecursiveExtensionFilteredLister;
import ingestion.other.utils.K;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.RelationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Map;

/**
 * This is the main class of our data source.
 *
 */
public class ExifDirectoryDataSource implements RelationProvider {
    private static Logger log = LoggerFactory.getLogger(
            ExifDirectoryDataSource.class);

    /**
     * Creates a base relation using the Spark's SQL context and a map of
     * parameters (our options)
     */
    @Override
    public BaseRelation createRelation(
            SQLContext sqlContext,
            Map<String, String> params) {
        log.debug("-> createRelation()");

        java.util.Map<String, String> optionsAsJavaMap =
                mapAsJavaMapConverter(params).asJava();

        // Creates a specifif EXIF relation
        ExifDirectoryRelation br = new ExifDirectoryRelation();
        br.setSqlContext(sqlContext);

        // Defines the process of acquiring the data through listing files
        RecursiveExtensionFilteredLister photoLister =
                new RecursiveExtensionFilteredLister();
        for (java.util.Map.Entry<String, String> entry : optionsAsJavaMap
                .entrySet()) {
            String key = entry.getKey().toLowerCase();
            String value = entry.getValue();
            log.debug("[{}] --> [{}]", key, value);
            switch (key) {
                case K.PATH:
                    photoLister.setPath(value);
                    break;

                case K.RECURSIVE:
                    if (value.toLowerCase().charAt(0) == 't') {
                        photoLister.setRecursive(true);
                    } else {
                        photoLister.setRecursive(false);
                    }
                    break;

                case K.LIMIT:
                    int limit;
                    try {
                        limit = Integer.valueOf(value);
                    } catch (NumberFormatException e) {
                        log.error(
                                "Illegal value for limit, expecting a number, got: {}. {}. Ignoring parameter.",
                                value, e.getMessage());
                        limit = -1;
                    }
                    photoLister.setLimit(limit);
                    break;

                case K.EXTENSIONS:
                    String[] extensions = value.split(",");
                    for (int i = 0; i < extensions.length; i++) {
                        photoLister.addExtension(extensions[i]);
                    }
                    break;

                default:
                    log.warn("Unrecognized parameter: [{}].", key);
                    break;
            }
        }

        br.setPhotoLister(photoLister);
        return br;
    }

}