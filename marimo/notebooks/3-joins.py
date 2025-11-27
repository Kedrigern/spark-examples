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
    # Table joins in PySpark

    ## 1:n relation

    First we can create classic left join. So there are multiple lines from left table if there are more than one match from right table.
    """)
    return


@app.cell
def n_1rel():
    from pyspark.sql.functions import col, count, collect_list, collect_set, sum as ssum

    path_u = "data/user/user.parquet"
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

    # Join
    df_u_i = df_u.alias("u").join(
        df_items.alias("i"), 
        on=col("u.id") == col("i.user_id"), 
        how="left"
    ).select(["u.id", "u.name", 'u.email', col("i.item"), col("i.id").alias("id_item")]).sort("id")

    df_u_i.toArrow()
    return col, collect_list, collect_set, count, df_u_i, ssum


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Group by

    [GroupBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html#pyspark.sql.DataFrame.groupBy) returns special object which anticipate methods `agg`, `count` etc.
    """)
    return


@app.cell
def _(col, collect_list, collect_set, count, df_u_i):
    # There must be all columns that you want to preserve in result
    df_u_i.groupBy(
        "id",
        "name",
        "email",
    ).agg(
        count("item").alias("item_count"),
        collect_list("item").alias("items"),
        collect_set("id_item").alias("items_id"),
    ).sort(col("item_count").desc(), col("id").asc()).toArrow()
    return


@app.cell(hide_code=True)
def _():
    mo.md(rf"""
    ## m:n relation

    For `m:n` relation we need litle more complex data. We use Books, Authors and relation between them. Data are prepared in parquet files.

    {mo.mermaid(book_graph.value).text}
    """)
    return


@app.cell
def read_book():
    books = spark.read.parquet("data/book/book.parquet") 
    books.toArrow()
    return (books,)


@app.cell
def read_authors():
    authors = spark.read.parquet("data/book/author.parquet")
    authors.toArrow()
    return (authors,)


@app.cell
def read_rel():
    book_author_rel = spark.read.parquet("data/book/book-author-rel.parquet")
    book_author_rel.toArrow()
    return (book_author_rel,)


@app.cell
def join_b_a(authors, book_author_rel, books, col):
    base_df = books.alias("b").join(
        book_author_rel.alias("r"),
        on=books.id == book_author_rel.book_id,
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
    return (base_df,)


@app.cell
def group_by_b_a(base_df, col, collect_list):
    base_df.groupby(
            col("b.id"),
            col("b.title"),
            col("b.series"),
            col("b.published"),
            col("b.pages"),
        ).agg(
            collect_list("a.name").alias("author")
        ).toArrow()
    return


@app.cell
def agg_func(base_df, col, collect_list, count, ssum):
    base_df.filter("b.series IS NOT NULL").groupBy(col("b.series")).agg(
        collect_list(col("b.title")),
        count("b.pages").alias("book_number"),
        ssum("b.pages").alias("total_pages")
    ).toArrow()
    return


if __name__ == "__main__":
    app.run()
