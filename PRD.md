# Explainable Schema Drift Analyzer for CSV Datasets

## 1. Project Title
**Explainable Schema Drift Analyzer for CSV Datasets**

## 2. Problem Statement
In real-world systems, datasets often change over time. Columns may be renamed, removed, added, reordered, or converted to different data types. Existing comparison tools usually show only raw differences and do not explain structural changes clearly. This project aims to build a Python-based tool that compares two versions of a CSV dataset and generates an explainable report describing schema-level and content-level drift.

## 3. Objective
The objective of this project is to detect and explain differences between two CSV files by analyzing:
- schema changes
- column-level changes
- type changes
- missing or new fields
- row-level differences based on keys
- possible causes of drift

## 4. Proposed Solution
The system will take two CSV files as input:
- an older dataset version
- a newer dataset version

It will analyze both files and generate a structured report containing:
- columns added or removed
- likely renamed columns
- data type changes
- reordered columns
- differences in record counts
- changed values in matching rows
- summary of the impact of the drift

## 5. Key Features
- Compare two CSV datasets
- Detect schema changes automatically
- Identify added, removed, and renamed columns
- Detect data type drift
- Compare common rows using a selected key column
- Generate markdown, text, or JSON report
- Highlight high-impact differences
- Optional AI-based plain-English summary

## 6. Technologies Used
- **Programming Language:** Python
- **Libraries:** Pandas, NumPy
- **Concepts:** OOP, File Handling, Data Comparison, Report Generation
- **Optional:** difflib / fuzzy matching for renamed columns

## 7. OOP Design
The project can be divided into the following classes:

### `CSVLoader`
Responsible for reading CSV files and validating input.

### `SchemaAnalyzer`
Detects schema differences such as added, removed, and reordered columns.

### `TypeChecker`
Compares data types and identifies type drift.

### `RowComparator`
Compares row values using a common key column.

### `ReportGenerator`
Creates the final report in markdown, text, or JSON format.

## 8. Input and Output
### Input
- `old_version.csv`
- `new_version.csv`

### Output
- Drift summary report
- Detailed column change report
- Optional row-level mismatch file

## 9. Modules
1. File Input Module
2. Schema Analysis Module
3. Type Comparison Module
4. Row Difference Module
5. Report Generation Module

## 10. Scope of the Project
This project is useful in:
- data migration verification
- dataset version comparison
- ETL pipeline validation
- academic and business record management

## 11. Advantages
- Solves a modern and practical data problem
- More unique than common dataset analysis projects
- Strong use of OOP, file handling, Pandas, and NumPy
- Easy to explain in project presentation and viva
- Can be completed as an MVP within limited time

## 12. Limitations
- Requires structured CSV input
- Renamed column detection may not always be perfect
- Row comparison depends on availability of a common key

## 13. Future Enhancements
- Add GUI support
- Support Excel files
- Add stronger fuzzy matching for columns
- Generate visual drift dashboard
- Add AI-generated explanation and recommendations

## 14. Conclusion
The Explainable Schema Drift Analyzer is a meaningful and modern mini project that solves an important data comparison problem. It combines OOP, file handling, Pandas, and NumPy in a practical way and provides a clear advantage over ordinary CSV comparison approaches by making the detected changes understandable.

