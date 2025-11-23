## Databricks DLT & Orchestrace – Cheat Sheet

### 1. Co je to Delta Live Tables (DLT)?

Není to formát souborů, je to **framework** (způsob řízení).

* **Fyzická vrstva:** Na disku (S3/ADLS) je to **obyčejná Delta Table** (Parquet + `_delta_log`).
* **Logická vrstva:** Je to tabulka "vlastněná" DLT enginem.
* **Pravidlo zámku:**
    * **Čtení (SELECT):** Odkudkoliv (Notebook, Power BI, SQL Endpoint).
    * **Zápis (INSERT/UPDATE):** Zakázáno zvenčí. Zapisovat smí pouze ta DLT pipeline, která tabulku stvořila.

---

### 2. Typy objektů v DLT
V DLT kódu definujete, jak se má tabulka chovat:

| Typ | Python dekorátor | Chování | Použití |
| :--- | :--- | :--- | :--- |
| **Streaming Table** | `@dlt.table` (přes `readStream`) | **Append-only**. Data se jen přihazují. Staví na Structured Streaming. | **Bronze** (Ingesce), logy. |
| **Materialized View** (Live Table) | `@dlt.table` (bez streamu) | **Current State**. Tabulka vždy odpovídá výsledku dotazu. DLT ji přepočítává (buď celou, nebo inkrementálně). | **Silver/Gold** (Aggregace, Joiny). |
| **View** | `@dlt.view` | **Virtual**. Data se neukládají, jen logický pohled. | Pomocné výpočty, mezikroky. |

---

### 3. Orchestrace: Hierarchie "Kdo koho spouští"

Představ si staveniště.

#### Level 1: Job (Workflow) – "Manažer"
* **Role:** Orchestrátor. Řeší **KDY** a **KDE** (Compute).
* **Funkce:** Plánovač (Schedule), Notifikace, Retry policy, Cluster config.
* **Obsah:** Skládá se z jednoho nebo více Tasků.

#### Level 2: Task – "Úkol v seznamu"
* **Role:** Jedna konkrétní činnost v rámci Jobu.
* **Typy:**
    * Run Notebook.
    * Run JAR.
    * **Run DLT Pipeline**.

#### Level 3: DLT Pipeline – "Továrna na data"

* **Role:** Zapouzdřená logika datových toků (Bronze -> Silver -> Gold).
* **Logika:** Řeší závislosti mezi tabulkami (Lineage).
* **Vztah:** Pro Job je DLT Pipeline jen jedna "černá skříňka" (jeden Task), která se musí dokončit.

---

### 4. Rozdíl v myšlení (Mental Shift)

| Klasický přístup (Notebooks) | DLT přístup (Pipelines) |
| :--- | :--- |
| **Imperativní (JAK):** "Načti stream, udělej merge, ulož checkpoint." | **Deklarativní (CO):** "Chci tabulku B, která vznikne vyčištěním tabulky A." |
| **Řízení závislostí:** Ručně (Task 1 -> Task 2). | **Automaticky:** DLT pozná, že Tabulka B potřebuje A. |
| **CDC (Upserty):** Složitý kód (`foreachBatch` + `merge`). | **Jednoduché:** `dlt.apply_changes()`. |

### 5. Produkční pattern

1.  Napíšete **DLT Pipeline** (kód v Pythonu/SQL).
2.  Vytvoříte **Job**.
3.  Do Jobu přidáte **Task** typu "Delta Live Tables" a vyberete svou pipeline.
4.  (Volitelně) Přidáte další Task (např. "Refresh Dashboard"), který se spustí až po úspěchu DLT.
