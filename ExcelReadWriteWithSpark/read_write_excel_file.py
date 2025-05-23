from pyspark.sql import SparkSession


def read_write_excel_file():
    spark = SparkSession.builder.appName("read_write_excel_file").master("local").config(
        "spark.jars.packages", "com.crealytics:spark-excel_2.12:3.3.2_0.19.0").getOrCreate()
    input_path = "C:\\Users\\lenovo\\Downloads\\input1.xlsx"
    output_path = "C:\\Users\\lenovo\\Downloads\\output1.xlsx"

    df = spark.read.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").load(input_path)

    df.printSchema()

    df.write.format("com.crealytics.spark.excel").option("header", "true").option("inferSchema", "true").mode("overwrite").save(output_path)


if __name__ == '__main__':
    read_write_excel_file()
