# Final Execution Checklist

Before submission/viva, verify on the Windows `bigdata` environment:

- [ ] `python -m pytest .\tests -q` passes.
- [ ] MongoDB service is Running and ping succeeds.
- [ ] Small file automatically selects `python_batch`.
- [ ] Large 12+ GB file `--route-only` automatically selects `pyspark`.
- [ ] Raw-only run shows every input row in `orders_raw` before quality.
- [ ] Quality demo shows Valid + Corrected + Quarantined.
- [ ] Audit Trail appears in corrected records.
- [ ] Quarantine keeps codes, details, and Raw record.
- [ ] Consistency equation passes for the run.
- [ ] Same input rerun does not increase final business records.
- [ ] Update test changes one existing record without Duplicate.
- [ ] Controlled PySpark run completes and Spark UI is captured.
- [ ] `reports/results.json` contains the latest run evidence.
- [ ] Required Compass/Spark screenshots are saved under `reports/screenshots/`.
- [ ] README command works from a clean PowerShell session.
- [ ] If submitted as a 2-person group, run and document Advanced Path B (or the registered Path A).
