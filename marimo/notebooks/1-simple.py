import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    # Initialization code that runs before all other cells
    import marimo as mo


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Simple PySpark

    Simple PySpark in Marimo. Including showing DataFrame in Marimo. It is neccesary to call `df.toArrow()`
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
def _():
    mo.md(r"""
    Now we have `spark` inicialized. So we can create first dataframe. In following notebookes initialization of spark will be moved to setup cell.

    Standard output is writen after cell and dataframe in last cell is displayed as table. Sometimes pyspark dataframe is not showed directly. Solution is to convert it to arrow format: `df.toArrow()`
    """)
    return


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
