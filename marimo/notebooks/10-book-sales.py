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

    book_graph = mo.ui.code_editor(
        value="""flowchart TD
        R[fa:fa-table Book author rel] --> B[fa:fa-book Book] 
        R  --> A[fa:fa-user Author]""",
        language="md",
        label="Mermaid editor",
    )


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
    books = spark.read.parquet("data/book/book.parquet") 
    authors = spark.read.parquet("data/book/author.parquet")
    ba_rel = spark.read.parquet("data/book/book-author-rel.parquet")
    base_df = books.alias("b").join(
        ba_rel.alias("r"),
        on=books.id == ba_rel.book_id,
        how="right").join(
        authors.alias("a"),
        on=col("r.author_id") == col("a.id"),
        how="left").select(
            col("b.id"),
            col("b.title"),
            col("b.series"),
            col("b.published"),
            col("b.pages"),
            col("a.name"),
            col("r.author_id"),
        )
    base_df.toArrow()

    return authors, ba_rel, base_df


@app.cell
def _():
    df_s = spark.createDataFrame(data=[
        [1, 1, 100, Decimal("399.50"), date(2025, 1, 1), date(2025, 6, 30)],
        [2, 2, 50, Decimal("499.00"), date(2025, 1, 1), date(2025, 6, 30)],
        [3, 1, 150, Decimal("370.00"), date(2025, 6, 30), date(2025, 12, 31)],
        [4, 3, 300, Decimal("400"), date(2025, 6, 30), date(2025, 12, 31)]
    ], schema="id INT, book_id INT, amount INT, price_avg DECIMAL(10,2), range_start DATE, range_end DATE")


    #books.toArrow()
    df_s.toArrow()
    return (df_s,)


@app.cell
def _(authors, ba_rel, base_df, df_s):
    def get_sales(author: str, range_start: date, range_end: date):
        base_df.filter(f"name = '{author}'")
        # For author
        author_row = authors.filter(f"name = 'George Orwell'").first()
        if not author_row:
            return
    
        author_id = author_row['id']
        print(author_id)

    
        book_ids = ba_rel.filter(ba_rel.author_id == author_id).agg(sf.collect_set(ba_rel.book_id)).first()[0]

        for book_id in book_ids:
            df = df_s.filter((df_s.book_id == book_id) & (df_s.range_start == range_start) & (df_s.range_end == range_end))
            return df.toArrow()
    
    get_sales("George Orwell", date(2025,1,1), date(2025,6,30))
    return


if __name__ == "__main__":
    app.run()
