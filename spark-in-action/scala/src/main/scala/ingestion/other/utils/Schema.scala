package ingestion.other.utils

import org.apache.spark.sql.types.StructType

import java.util

@SerialVersionUID(2376325490075130182L)
class Schema extends Serializable {
  private val columns = new util.HashMap[String, SchemaColumn]()
  private var structSchema: StructType = new StructType()

  def getSparkSchema: StructType = structSchema

  def setSparkSchema(structSchema: StructType): Unit = {
    this.structSchema = structSchema
  }

  def add(col: SchemaColumn): Unit = {
    columns.put(col.getColumnName, col)
  }

  def getMethodName(columnName: String): String =
    columns.get(columnName).getMethodName
}
