from pyspark.sql import SparkSession

# Створюємо екземпляр SparkSession
spark = SparkSession.builder \
    .appName("Example") \
    .config("some-config", "config-value") \
    .getOrCreate()

#
# Tіло програми 
#

# Завжди хороша практика закрити SparkSession наприкінці програми
spark.stop()

