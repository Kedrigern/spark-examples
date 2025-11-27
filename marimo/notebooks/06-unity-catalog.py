import marimo

__generated_with = "0.18.1"
app = marimo.App(width="medium")

with app.setup(hide_code=True):
    # Initialization code that runs before all other cells
    import marimo as mo


@app.cell(hide_code=True)
def _():
    mo.md(r"""
 
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Unity catalog

    Spark can cooperate with many catalog. So far we used in-memory catalog (notebook `4-tables.py`).

    Unity Catalog (`uc`) is used in Databricks and is widespread. It allows
    """)
    return


@app.cell
def _():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    import os

    def prepare_spark_with_uc(uc_uri: str) -> SparkSession:
        builder = (
            SparkSession.builder.appName("PySparkWithUC")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalog.uc", "io.unitycatalog.spark.UCSingleCatalog")
            .config("spark.sql.catalog.uc.uri", uc_uri)
            .config("spark.sql.catalog.uc.token", "not-used-in-local-mode") 
            #.config("spark.ui.showConsoleProgress", "false")
            #.config("spark.log.level", "ERROR")
        )

        # https://mvnrepository.com/artifact/io.unitycatalog/unitycatalog-spark
        my_packages = ["io.unitycatalog:unitycatalog-spark_2.12:0.3.0"]

        spark = configure_spark_with_delta_pip(builder, extra_packages=my_packages).getOrCreate()
        #spark.sparkContext.setLogLevel("ERROR")
        return spark

    spark = prepare_spark_with_uc("http://localhost:8080")

    #print(spark.conf.get("spark.sql.catalogImplementation"))

    #for c in spark.catalog.listCatalogs():
    #    print(c)

    #spark.sql("SHOW CATALOGS").show()
    # TEST:
    # Vytvoření namespace (schema) v UC
    #spark.sql("CREATE NAMESPACE IF NOT EXISTS uc.my_schema")
    # Vytvoření tabulky
    #spark.sql("CREATE TABLE IF NOT EXISTS uc.my_schema.numbers (id INT) USING DELTA")
    return


if __name__ == "__main__":
    app.run()
