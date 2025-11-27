import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo

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

    spark: SparkSession = prepare_spark()


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Data transformation in PySpark
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## DataFrame without proper datatypes
    """)
    return


@app.cell
def _():
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType
    )

    string_user_schema = StructType(
        [
            StructField("id_s", StringType(), True),
            StructField("name_s", StringType(), True),
            StructField("email_s", StringType(), True),
            StructField("birthday_s", StringType(), True),
            StructField("registered_at_s", StringType(), True),
            StructField("is_active_s", StringType(), True),
            StructField("balance_s", StringType(), True),
            StructField("transparency_level_s", StringType(), True),
        ]
    )
    path = "data/user/*.csv" # all csv files in directory
    df = (
        spark.read.format("csv")
        .option("header", True)
        .schema(string_user_schema)
        .load(path)
    )
    df.toArrow()
    return (df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Type casting

    Cast columns (all are in string) to its proper data types. We use mass operation. You can perform it by chaining too:

    `df2 = df.withColumn("id", df.id_s.cast("int")).withCo...  `

    `df2 = df.withColumnRenamed("name_s", "name")`
    """)
    return


@app.cell
def _(df):
    import pyspark.sql.functions as sf
    from pyspark.sql.types import DecimalType, DoubleType

    df2 = df.withColumns(
        {
            "id": df.id_s.cast("int"),
            "name_u": sf.upper(df.name_s),
            "domain": sf.split(df.email_s, "@")[1],
            "birthday": df.birthday_s.cast("date"),
            "registered_at": df.registered_at_s.cast("timestamp"),
            "is_active": df.is_active_s.cast("boolean"),
            "balance": df.balance_s.cast(DecimalType(10, 2)),
            "transparency_level": df.transparency_level_s.cast(DoubleType()),
        }
    ).withColumnsRenamed({"name_s": "name", "email_s": "mail"}).select(
        [
            "id",
            "name",
            "mail",
            "birthday",
            "registered_at",
            "is_active",
            "balance",
            "transparency_level",
        ]
    )

    df2.toArrow()
    return df2, sf


@app.cell
def _(df2, sf):
    from pyspark.sql.functions import col

    interests_rate = 1.04

    df3 = df2.withColumns(
        {
            "name_u": sf.upper(col("name")),
            "domain": sf.split(col("mail"), "@")[1],
            "balance_after_interests": col("balance") * interests_rate
        }
    )

    df3.toArrow()
    return


if __name__ == "__main__":
    app.run()
