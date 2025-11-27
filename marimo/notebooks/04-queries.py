import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    import marimo as mo

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql.classic.dataframe import DataFrame
    from pyspark.sql.functions import col
    import pyspark.sql.functions as sf
    from decimal import Decimal
    from datetime import datetime, date


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


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Queris
    """)
    return


@app.cell
def load_data():
    path = "data/user/user.parquet"
    users = spark.read.parquet(path)

    users.toArrow()
    return (users,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Filtering

    `df.filter()` returns filtred dataframe (in our case return dataframe with 1 row.

    We can call `df.filter(...).first()` to get row (`pyspark.sql.types.Row`).

    [Filtering](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html) can be:

    - string is SQL expression
    - python expresion is when we use type column, like: `users.name` or `col("name")`
    """)
    return


@app.cell
def _(users):
    row_lily1 = users.filter("name = 'Lily'").first()       # SQL expression
    row_lily2 = users.filter(users.name == "Lily").first()  # Python expression
    lily_id = row_lily1['id']

    assert row_lily1 == row_lily2
    assert isinstance(lily_id, int)

    df = users.filter((users.name == "Lily") | (users.name == "James"))
    assert df.count() == 2

    df = users.filter((users.name == "Lily") & (users.name == "James"))
    assert df.count() == 0

    # is in
    df = users.filter(users.name.isin("Lily", "James"))
    assert df.count() == 2 # just Lily and James

    # negation
    df = users.filter(~users.name.isin("Lily", "James"))
    assert df.count() == 4 # everybody except Lily and James (6-2=4)

    # like, also ilike, rlike
    df = users.filter(users.name.like("Al%"))
    assert df.count() == 1 # just Alice

    # contains
    df = users.filter(users.name.contains("Al"))
    assert df.count() == 1 # just Alice

    # between
    df = users.filter(users.id.between(1,2))
    assert df.count() == 2 # id 1, 2

    # endswith
    df = users.filter(users.email.endswith("@example.com"))
    assert df.count() == 3 # all mails at example.com domain
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Sorting

    Sorting is very simple. Argument is column or columns. If there is more than one columns it is sorted by them from left side. We can determine if desc() or asc()
    """)
    return


@app.cell
def _(users):
    users.sort(users.is_active.desc(), users.balance.asc())
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Pagination

    There is `offset` and `limit` function. But be carefoul it depend at order. These two are not same:

    ```python
    users.offset(3).limit(2)
    users.limit(2).offset(3)
    ```
    """)
    return


@app.cell
def _(users):
    users.offset(3).limit(2) 
    return


if __name__ == "__main__":
    app.run()
