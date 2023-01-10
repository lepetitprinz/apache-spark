package ingestion.other.utils;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Simple annotation to extend and ease the Javabean metadata when
 * converting to a Spark column in a dataframe.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface SparkColumn {

    /**
     * The name of the column can be overriden.
     */
    String name() default "";  // Overrides the column name

    /**
     * Forces the data type of the column
     */
    String type() default "";  // Overrides the column type

    /**
     * Forces the required/nullable property
     */
    boolean nullable() default true;  // Sets the nullable property
}