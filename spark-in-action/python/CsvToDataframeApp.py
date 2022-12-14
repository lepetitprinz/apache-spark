import os
from pyspark.sql import SparkSession

curr_dir = os.path.dirname(__file__)
relative_path = os.path.join("..", "..", "..", "..", "data", "books.csv")
absolute_path = os.path.join(curr_dir, relative_path)

# create a session on a local master
session = SparkSession.builder
	.appName("CSV to Dataset")
	.master("local[*]")
	.getOrCreate()

# reads a CSV file with header and stores it in a dataframe
df = session.read.csv(header=True, inferSchema=True, path=absolute_path)

# show at most 5 rows from the dataframe
df.show(5)

# stop SparkSession at the end of the application
session.stop()