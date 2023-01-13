package ingestion.other.utils

class SchemaColumn {
  var methodName = ""
  var columnName = ""

  def setMethodName(methodName: String): Unit = {
    this.methodName = methodName
  }

  def setColumnName(columnName: String): Unit = {
    this.columnName = columnName
  }

  def getMethodName: String = {
    this.methodName
  }
  def getColumnName: String = {
    this.columnName
  }
}

