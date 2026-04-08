# Additional CSV Test Cases

This folder contains extra old/new CSV pairs for testing schema and row drift behavior.

- `basic_add_remove_old.csv` vs `basic_add_remove_new.csv`
  - Added/removed columns and one likely rename (`name` -> `full_name`)
- `rename_reorder_old.csv` vs `rename_reorder_new.csv`
  - Column reorder and likely rename (`amount_usd` -> `total_amount`)
- `type_drift_old.csv` vs `type_drift_new.csv`
  - Semantic type drift examples (numeric/bool/date represented as text)
- `row_diff_key_old.csv` vs `row_diff_key_new.csv`
  - Missing/new keys and changed values for row-level comparison

Example runs:

```bash
drift analyze examples/test_cases/basic_add_remove_old.csv examples/test_cases/basic_add_remove_new.csv
drift analyze examples/test_cases/row_diff_key_old.csv examples/test_cases/row_diff_key_new.csv --key txn_id
```
