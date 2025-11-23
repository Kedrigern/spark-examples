## Databricks Ingesce & Upsert – Cheat Sheet

### 1\. Mentální model

Rozděl si proces na dvě fáze:

  * **Ingesce (Čtení):** Jak efektivně načíst nové soubory? -\> **Auto Loader** (nebo COPY INTO).
  * **Akce (Zápis):** Jak data aplikovat do tabulky? -\> **MERGE INTO** (Upsert).

### 2\. Ingesce: Načítání dat

#### A) COPY INTO (SQL)

  * **Kdy:** Ad-hoc, statické schéma, tisíce souborů (ne miliony).
  * **Vlastnost:** Idempotentní (pamatuje si soubory), jednoduché.

```sql
COPY INTO my_catalog.target_table
FROM 's3://bucket/path/'
FILEFORMAT = JSON
FORMAT_OPTIONS ('mergeSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

#### B) Auto Loader (Python / `cloudFiles`)

  * **Kdy:** Produkční standard. Streaming i Batch. Schema Evolution. Miliony souborů.
  * **Syntaxe:** `format("cloudFiles")`

```python
df_stream = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json") 
  .option("cloudFiles.schemaLocation", "s3://.../schemas/my_table/") # Nutné pro evoluci
  .option("cloudFiles.inferColumnTypes", "true")
  .load("s3://.../source_data/")
)
```

-----

### 3\. The "Pro" Pattern: Auto Loader + MERGE

Jak spojit streamované čtení s upsertem (batch operací).

**Princip:** `readStream` (Auto Loader) -\> `foreachBatch` -\> `MERGE` (Delta Table).

```python
from delta.tables import *

# 1. Funkce pro Micro-batch
def upsert_func(batch_df, batch_id):
    if batch_df.count() == 0: return
    
    target_table = DeltaTable.forName(spark, "target_table")
    (target_table.alias("t")
      .merge(batch_df.alias("s"), "t.id = s.id") # Join condition
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute()
    )

# 2. Spuštění (Batch mode via availableNow)
(df_stream.writeStream
  .foreachBatch(upsert_func)
  .option("checkpointLocation", "s3://.../checkpoints/my_table/") # Nutné!
  .trigger(availableNow=True) # Zpracuj vše a vypni se
  .start()
)
```

-----

### 4\. Infrastruktura (Složky)

Tyto cesty definujete v `.option()`.

| Pojem | Typ | Co to je | Pravidlo |
| :--- | :--- | :--- | :--- |
| **Checkpoint** | Složka | **"Záložka v knize"**. Obsahuje offsety a commity. Říká streamu, kde skončil a co je hotové. | **NESAHAT.** Smazat jen pokud chcete resetovat stream od nuly. |
| **Schema Loc.** | Složka | **"Historie struktury"**. Auto Loader si sem ukládá změny schématu JSONů v čase. | **NESAHAT.** Smazat při resetu streamu. |

-----

### 5\. Vývojářský "Reset" (Snippet)

Když ladíte a chcete pustit kód znovu „od čistého stolu“, musíte smazat data I metadata.

```python
# Pomocná funkce pro vyčištění prostředí při vývoji
def reset_dev_env(table_name, checkpoint_path, schema_path):
    # 1. Smazat metadata streamu
    dbutils.fs.rm(checkpoint_path, recurse=True)
    dbutils.fs.rm(schema_path, recurse=True)
    
    # 2. Smazat tabulku (volitelně i data, pokud je managed)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    print(f"Prostředí pro {table_name} bylo kompletně vyčištěno.")

# Použití:
# reset_dev_env("my_silver_table", "s3://.../checkpoints/", "s3://.../schemas/")
```
