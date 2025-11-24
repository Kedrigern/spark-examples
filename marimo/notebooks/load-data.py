import marimo

__generated_with = "0.18.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Load data with PySpark

    Spark inicialization is very verbose. Don't be suprise by warnings.
    """)
    return


@app.cell
def _():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql.classic.dataframe import DataFrame

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

    spark = prepare_spark()
    return (spark,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Source Files

    We have same data in 3 format: `csv`, `json`, `parquet`. CSV is split into 2 files.
    """)
    return


@app.cell
def source_files_py():
    import os

    base_path = "data/"

    print("Size Filename")
    for file in os.listdir(base_path):
        result = os.stat(base_path + file)
        print(f"{result.st_size}\t {file} ")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Load from app

    Simple on the fly dataframe creation. Last dataframe is showed. It is better to convert it to arrow format, because it shows datatypes in Marimo.
    """)
    return


@app.cell
def load_from_app(spark):
    data = [
        [1, "Alice", "1989-01-15"],
        [2, "Bob", "1995-02-20"],
        [3, "Cathy", "1988-03-10"]
    ]
    df1 = spark.createDataFrame(data, ["id", "name", "birthday"])
    print(type(df1))
    df1.toArrow()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Load from CSV

    Load one csv file with 3 users.
    """)
    return


@app.cell
def load_from_csv_1(spark):
    path2 = "data/user.csv"
    df2 = (
        spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(path2)
    )

    df2.toArrow()
    return (path2,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    In csv there is no info about datatypes, so decimal type is wrongly cast as Float64 that can lead to precision lost. We have to define schema to obtain correct datatypes in columns.
    """)
    return


@app.cell
def load_from_csv_2(path2, spark):
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        DecimalType,
        IntegerType,
        BooleanType,
        DoubleType,
        DateType,
        TimestampType,
    )

    user_schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("birthday", DateType(), True),
            StructField("registered_at", TimestampType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("balance", DecimalType(10, 2), True),
            StructField("transparency_level", DoubleType(), True),
        ]
    )

    df2b = (
        spark.read.format("csv")
        .option("header", True)
        .schema(user_schema)
        .load(path2)
    )

    df2b.toArrow()
    return (user_schema,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Load directory

    Loads two csv (`data/user.csv`, `data/user2.csv`) into one table. Each contains 3 users.
    """)
    return


@app.cell
def load_from_csv_3(spark, user_schema):
    path3 = "data/*.csv" # all csv files in directory
    df3 = (
        spark.read.format("csv")
        .option("header", True)
        .schema(user_schema)
        .load(path3)
    )
    df3.toArrow()


    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Load json

    JSON is not ideal format too. So without schema some columns are not casted correctly. But decimal is interpreted as string, so there is precision lost.
    """)
    return


@app.cell
def load_from_json(spark):
    path4 = "data/user.json"
    df4 = (
        spark.read.format("json")
        .load(path4)
    )
    df4.toArrow()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Load parquet

    Parquet is only format which datatypes included. Normaly is also smalest one thanks to compresion. In this case not, because small amount of data.
    """)
    return


@app.cell
def load_from_parquet(spark):
    path5 = "data/user.parquet"
    df5 = (
        spark.read.format("parquet")
        .load(path5)
    )

    df5.toArrow()
    return


if __name__ == "__main__":
    app.run()
