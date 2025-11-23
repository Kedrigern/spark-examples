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
    # Table joins in PySpark
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


@app.cell
def _(spark):
    data = [
        [1, "Alice"], [2, "Bob"], [3, "Cathy"]
    ]
    df = spark.createDataFrame(data, ["id", "name"])
    print("Hello world!")
    print(type(df))
    df.toArrow()
    return


if __name__ == "__main__":
    app.run()
