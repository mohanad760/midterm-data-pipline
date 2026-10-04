# Data Pipeline Execution Metrics Report

**Generated:** 2026-10-04 15:32:52 UTC

## Summary of Pipeline Runs

| Run ID | Engine | File | Size (MB) | Raw Loaded | Valid | Corrected | Quarantine | Elapsed (s) | Speed (rows/s) | Upsert (Ins/Upd/Unch) |
| :--- | :---: | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| `batch_8b742f504a89` | python_batch | orders_sample_100000.csv | 41.67 | 100,000 | 0 | 90,556 | 9,444 | 49.78 | 2,009 | 89937 / 619 / 0 |
| `spark_ba394e984d9e` | pyspark | orders_sample_100000.csv | 41.67 | 10,000 | 0 | 9,043 | 957 | 86.11 | 116 | 0 / 9043 / 0 |
| `batch_87667d3db0d5` | python_batch | orders_sample_100000.csv | 41.67 | 100,000 | 0 | 90,556 | 9,444 | 31.32 | 3,193 | 89937 / 619 / 0 |
| `spark_81e6efa8ef23` | pyspark | orders_sample_100000.csv | 41.67 | 20,000 | 0 | 18,069 | 1,931 | 39.59 | 505 | 0 / 18069 / 0 |
| `spark_8e336f1dca81` | pyspark | orders_sample_100000.csv | 41.67 | 5,000 | 0 | 0 | 0 | 27.00 | 185 | 0 / 0 / 0 |

## Consistency Check Formula Verification
Condition: `run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count`

- **batch_8b742f504a89**: 100,000 Raw = 0 Valid + 90,556 Corrected + 9,444 Quarantined [PASSED]
- **spark_ba394e984d9e**: 10,000 Raw = 0 Valid + 9,043 Corrected + 957 Quarantined [PASSED]
- **batch_87667d3db0d5**: 100,000 Raw = 0 Valid + 90,556 Corrected + 9,444 Quarantined [PASSED]
- **spark_81e6efa8ef23**: 20,000 Raw = 0 Valid + 18,069 Corrected + 1,931 Quarantined [PASSED]
- **spark_8e336f1dca81**: 5,000 Raw = 0 Valid + 0 Corrected + 0 Quarantined [FAILED]