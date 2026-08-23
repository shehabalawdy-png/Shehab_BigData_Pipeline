# Advanced Path B — Incremental Loading

This extension is isolated from the mandatory core pipeline.

## Mechanism

A Delta CSV contains the normal 17 order columns plus a positive integer
`version`. The module loads every Delta row to Raw first, runs the same quality
rules, then applies version-aware Upsert:

- no existing `id_order` -> `inserted`
- higher version -> `updated`
- same version + same payload -> `unchanged`
- lower version -> ignored as stale (`unchanged` + `stale_ignored`)
- same version + different payload -> Quarantine (`VERSION_CONFLICT_SAME_VERSION`)

## Three required demonstrations

1. Initial load:

```powershell
python -m src.main --input ".\demo\incremental_initial.csv" --database "shehab_incremental_demo"
```

2. Delta with Update + Insert:

```powershell
python -m src.incremental_loader --input ".\demo\incremental_delta_v2.csv" --database "shehab_incremental_demo"
```

3. Re-run the same Delta:

```powershell
python -m src.incremental_loader --input ".\demo\incremental_delta_v2.csv" --database "shehab_incremental_demo"
```

The second Delta execution must not add duplicate business records.
