from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = SparkSession.builder.appName("MyGoitSparkSandbox").getOrCreate()

# Завантажуємо датасет
nuek_df = spark.read.csv('./nuek-vuh3.csv', header=True)

# Завантажуємо датасет альтернативним методом (див. примітку нижче)
nuek_alt_read_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load('./nuek-vuh3.csv')

# Виводимо перші 10 записів
nuek_df.show(10)

# Створюємо тимчасове представлення для виконання SQL-запитів
nuek_df.createTempView("nuek_view")
# Виконуємо SQL-маніпуляції
spark.sql("SELECT * FROM nuek_view LIMIT 10").show()

# Закриваємо сесію Spark
spark.stop()
