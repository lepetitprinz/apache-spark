import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

curr_dir = os.path.dirname(__file__)
relative_path = os.path.join("..", "..", "..", "..", "data", "authors.csv")
absolute_path = os.path.join(curr_dir, relative_path)