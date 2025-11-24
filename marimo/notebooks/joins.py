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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## 1:n relation
    """)
    return


@app.cell
def _(spark):
    from pyspark.sql.functions import col

    path_u = "data/user.parquet"
    df_u = spark.read.format("parquet").load(path_u)

    items_data = [
        # id, user_id, item
        [1, 1, "Porcelain cup"],
        [2, 2, "USB stick"], 
        [3, 2, "Phone"],
        [4, 4, "Sword"],
        [5, 4, "Wine"]
    ]

    df_items = spark.createDataFrame(items_data, ["id", "user_id", "item"])

    df_u_i = df_u.alias("u").join(
        df_items.alias("i"), on=col("u.id") == col("i.user_id"), how="left"
    ).select(["u.id", "u.name", "u.email", "i.item"])

    df_u_i.toArrow()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Group by

    [groupBy doc](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html#pyspark.sql.DataFrame.groupBy)
    """)
    return


@app.cell
def _():
    #df_u_i.groupby("id")
    #df_u_i.groupBy("id")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## m:n relation
    """)
    return


@app.cell
def _():
    address_data = [
        [1, "5528 Joker St, Rapid City, SD 57703", "USA"],
        [2, "Windsor SL4 1NJ", "United Kingdom"],
        [3, "Spálená 16, 110 00 Nové Město", "Czech republic"],
    ]
    return


if __name__ == "__main__":
    app.run()
