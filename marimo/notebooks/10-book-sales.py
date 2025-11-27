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


@app.cell
def _():
    book_graph = mo.ui.code_editor(
        value="""flowchart TD
        R[fa:fa-table Book author rel] --> B[fa:fa-book Book] 
        R  --> A[fa:fa-user Author]""",
        language="md",
        label="Mermaid editor",
    )
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Book sales

    Let have data: Books, Authors and Sales. Book and Authors has m:n relation. One row of sales represents

    {mo.mermaid(book_graph.value).text}
    """)
    return


@app.cell
def _():
    df_s = spark.createDataFrame(data=[
        [1, 1, 100, Decimal("399.50"), date(2025, 1, 1), date(2025, 6, 30)],
        [2, 2, 50, Decimal("499.00"), date(2025, 1, 1), date(2025, 6, 30)],
        [3, 1, 150, Decimal("370.00"), date(2025, 1, 1), date(2025, 6, 30)],
    ], schema="id INT, book_id INT, amount INT, price_avg DECIMAL(10,2), range_start DATE, range_end DATE")


    #books.toArrow()
    df_s.toArrow()

    return


if __name__ == "__main__":
    app.run()
