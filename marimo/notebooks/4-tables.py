import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    # Initialization code that runs before all other cells
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

    spark = prepare_spark()


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Tables in PySpark

    ## Catalog

    Spark is now using in-memory catalog. This catalog will be deleted after end of session. But data are saved in `spark-warehouse`.

    For example in Databricks will use Unity catalog and it is generaly widespread. It allows managed storage localy and via network. Typicaly is used to connect cloud storages. From the user perspective catalog act as `database` in database system - basicaly it is namespace. No you have 3 level structure:

    ```
    .
    ├── catalog1
    │   ├── database1
    │   │   ├── table1
    │   │   ├── table2
    │   └── database2
    │       ├── table1
    │       └── function1
    └── catalog2
    ```
    """)
    return


@app.cell
def show_catalog():
    print(spark.conf.get("spark.sql.catalogImplementation"))

    for c in spark.catalog.listCatalogs():
        print(c)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Database

    Catalogs are divided into databases. From this level things are very similar to other SQL systems.
    """)
    return


@app.cell
def show_db():
    spark.sql("CREATE DATABASE IF NOT EXISTS library LOCATION 'database'")
    spark.sql("USE DATABASE library")

    for db in spark.catalog.listDatabases():
        print(db)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Create and read table

    Tables are save into: `<catalog>/<database>/<tables>`. In our case table `book` wiil be: `spark-warehouse/library/book/`

    In case of delta format table is directory containing parquet files and logs in json.
    """)
    return


@app.cell
def write_table():
    df = spark.read.format("parquet").load("data/book/book.parquet")
    df.write.saveAsTable(name="book", format="delta", mode="overwrite")

    # Now we can use classic SQL:
    spark.sql("SELECT * FROM book WHERE series = 'Discworld'").toArrow()
    return


if __name__ == "__main__":
    app.run()
