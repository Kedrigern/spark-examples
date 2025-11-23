import logging
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.classic.dataframe import DataFrame

logging.basicConfig(level=logging.ERROR)
logging.getLogger("py4j").setLevel(logging.ERROR)


def prepare_spark() -> SparkSession:
    """
    Prepare SparkSession, it is very verbose operation.
    :return: SparkSession
    """
    builder = (
        SparkSession.builder.appName("PySparkLocale")
        .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.log.level", "ERROR")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark



def main():
    spark = prepare_spark()
    data = [
        [1, "Alice"], [2, "Bob"], [3, "Cathy"]
    ]
    df = spark.createDataFrame(data, ["id", "name"])
    print("Hello world!")
    print(type(df))
    df.show()


if __name__ == "__main__":
    main()
