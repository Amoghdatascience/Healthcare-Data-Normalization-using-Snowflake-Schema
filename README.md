# Healthcare Data Normalization Project (PySpark)

## Project Overview
This project transforms a flat, legacy healthcare dataset into a normalized, snowflake schema using Apache PySpark. The input is a single CSV file containing raw healthcare visit records. The pipeline extracts and transforms this data into 11 normalized dimension and fact tables, saved as individual CSVs for analysis and visualization.

The dataset includes patient demographics, doctor details, insurance and billing information, diagnoses, prescriptions, treatments, lab orders, and visit records. The output supports efficient querying and integration with visualization tools like Tableau.

---

## Required Packages
- `pyspark`
- `os`, `hashlib`, `datetime` (Python standard libraries)

> To install PySpark, run:
```bash
pip install pyspark
```

---

## Running the Project

### Prerequisites
- Python 3.12
- Apache Spark 3.5+
- Input file at `./data/legacy_healthcare_data.csv`

### Instructions
1. Clone the repository
2. Ensure input data is in the `data` directory
3. Navigate to the `project-3` folder
4. Run the main script:
```bash
python3 srcmain.py
```

### Expected Output
- All 11 normalized CSV files will be saved to the `output/` folder
- Tables:
  - DimPatient
  - DimInsurance
  - DimBilling
  - DimProvider
  - DimLocation
  - DimPrimaryDiagnosis
  - DimSecondaryDiagnosis
  - DimTreatment
  - DimPrescription
  - DimLabOrder
  - FactVisit

Each file will contain uniquely identified and cleaned data relevant to its table’s purpose.

---

## Snowflake Schema

### Dimension Tables
| Table                   | Primary Key              | Description                                |
|------------------------|--------------------------|--------------------------------------------|
| DimPatient             | patient_id               | Patient information & status               |
| DimInsurance           | insurance_id             | Insurance info, linked via patient_id      |
| DimBilling             | billing_id               | Billing amounts & payment status           |
| DimProvider            | provider_id              | Doctor details                             |
| DimLocation            | location_id              | Clinic and room identifiers                |
| DimPrimaryDiagnosis    | primary_diagnosis_id     | Main diagnosis per visit                   |
| DimSecondaryDiagnosis  | secondary_diagnosis_id   | Optional secondary diagnosis               |
| DimTreatment           | treatment_id             | Treatment performed                        |
| DimPrescription        | prescription_id          | Medication prescribed                      |
| DimLabOrder            | lab_order_id             | Lab test data                              |

### Fact Table
| Table      | Primary Key | Foreign Keys                                                                 |
|------------|-------------|------------------------------------------------------------------------------|
| FactVisit  | visit_id    | patient_id, insurance_id, billing_id, provider_id, location_id, primary_diagnosis_id, secondary_diagnosis_id, treatment_id, prescription_id, lab_order_id |

---

## Data Quality & Validation
- Primary keys are generated using MD5 hashes
- Foreign key integrity is validated across all tables
- Missing/optional columns like `secondary_diagnosis_id` or `prescription_id` are handled gracefully

---

## Tableau Dashboard Observations

### Sheet 1: Patient Gender Distribution
- Nearly even male and female patient distribution
- Slightly more females than males

### Sheet 2: Monthly Visit Trends
- Emergency and Follow-up visits are most frequent
- Spikes observed in 2023 and early 2024

### Sheet 3: Average Billing by Visit Type
- Emergency visits are the costliest
- Specialist and Routine visits cost slightly less

### Sheet 4: Doctor Visit Counts
- Sharon Mcdaniel and Amanda Moran handle the most visits
- Balanced distribution among 8 listed doctors

### Sheet 5: Billing by Insurance Plan Type
- HMO plans result in the highest total billing
- Medicaid generates the least total billing

---

## Project Status
Actively maintained  
Fully functional  
Data validation implemented

Feel free to fork this repo or raise issues for feedback, improvements, or contributions.

### Contact
This project was done as a part of coursework under the guidance of Prof. Zimeng Lyu at Rochester Institute of Technology.

