package ingestion.other.utils

import java.lang.annotation.{Retention, RetentionPolicy}

/**
 * Simple annotation to extend and ease the Javabean metadata when
 * converting to a Spark column in a dataframe.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
trait SparkColumn {
  /**
   * The name of the column can be overriden.
   */
  def name: String

  /**
   * Forces the data type of the column
   */
  def `type`: String

  /**
   * Forces the required/nullable property
   */
  def nullable: Boolean
}
