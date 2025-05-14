import os
from pyspark.sql import SparkSession
from data_processer import *

INPUT_PATH = "./data/legacy_healthcare_data.csv"
OUTPUT_PATH = "./output/"


def main():
    print(f" INPUT_PATH: {INPUT_PATH}")
    print(f" OUTPUT_PATH: {OUTPUT_PATH}")

    print("Initializing Spark session...")
    spark = SparkSession.builder.appName("Healthcare ETL").getOrCreate()

    # Loading and cleaning
    raw_df = load_raw_data(spark, INPUT_PATH)
    clean_output_folder(OUTPUT_PATH)

    # Normalizing dimension tables
    dim_patient = normalize_dim_patient(raw_df)
    dim_insurance = normalize_dim_insurance(raw_df)
    dim_billing = normalize_dim_billing(raw_df)
    dim_provider = normalize_dim_provider(raw_df)
    dim_location = normalize_dim_location(raw_df)
    dim_primary_dx = normalize_dim_primary_diagnosis(raw_df)
    dim_secondary_dx = normalize_dim_secondary_diagnosis(raw_df)
    dim_treatment = normalize_dim_treatment(raw_df)
    dim_prescription = normalize_dim_prescription(raw_df)
    dim_lab_order = normalize_dim_lab_order(raw_df)

    fact_visit = normalize_fact_visit(
        raw_df,
        dim_provider,
        dim_location,
        dim_primary_dx,
        dim_secondary_dx,
        dim_treatment,
    )

    # Saving outputs
    save_to_csv(dim_patient, OUTPUT_PATH, "DimPatient")
    save_to_csv(dim_insurance, OUTPUT_PATH, "DimInsurance")
    save_to_csv(dim_billing, OUTPUT_PATH, "DimBilling")
    save_to_csv(dim_provider, OUTPUT_PATH, "DimProvider")
    save_to_csv(dim_location, OUTPUT_PATH, "DimLocation")
    save_to_csv(dim_primary_dx, OUTPUT_PATH, "DimPrimaryDiagnosis")
    save_to_csv(dim_secondary_dx, OUTPUT_PATH, "DimSecondaryDiagnosis")
    save_to_csv(dim_treatment, OUTPUT_PATH, "DimTreatment")
    save_to_csv(dim_prescription, OUTPUT_PATH, "DimPrescription")
    save_to_csv(dim_lab_order, OUTPUT_PATH, "DimLabOrder")
    save_to_csv(fact_visit, OUTPUT_PATH, "FactVisit")

    # Validation
    print("\n Performing Primary Key Validation...")
    validate_primary_key(dim_patient, "patient_id", "DimPatient")
    validate_primary_key(dim_insurance, "insurance_id", "DimInsurance")
    validate_primary_key(dim_billing, "billing_id", "DimBilling")
    validate_primary_key(dim_provider, "provider_id", "DimProvider")
    validate_primary_key(dim_location, "location_id", "DimLocation")
    validate_primary_key(dim_primary_dx, "primary_diagnosis_id", "DimPrimaryDiagnosis")
    validate_primary_key(
        dim_secondary_dx, "secondary_diagnosis_id", "DimSecondaryDiagnosis"
    )
    validate_primary_key(dim_treatment, "treatment_id", "DimTreatment")
    validate_primary_key(dim_prescription, "prescription_id", "DimPrescription")
    validate_primary_key(dim_lab_order, "lab_order_id", "DimLabOrder")
    validate_primary_key(fact_visit, "visit_id", "FactVisit")

    print("\n Performing Foreign Key Validation...")
    validate_foreign_key(
        fact_visit, dim_patient, "patient_id", "patient_id", "FactVisit", "DimPatient"
    )
    validate_foreign_key(
        fact_visit,
        dim_insurance,
        "insurance_id",
        "insurance_id",
        "FactVisit",
        "DimInsurance",
    )
    validate_foreign_key(
        fact_visit, dim_billing, "billing_id", "billing_id", "FactVisit", "DimBilling"
    )
    validate_foreign_key(
        fact_visit,
        dim_provider,
        "provider_id",
        "provider_id",
        "FactVisit",
        "DimProvider",
    )
    validate_foreign_key(
        fact_visit,
        dim_location,
        "location_id",
        "location_id",
        "FactVisit",
        "DimLocation",
    )
    validate_foreign_key(
        fact_visit,
        dim_primary_dx,
        "primary_diagnosis_id",
        "primary_diagnosis_id",
        "FactVisit",
        "DimPrimaryDiagnosis",
    )
    validate_foreign_key(
        fact_visit,
        dim_secondary_dx,
        "secondary_diagnosis_id",
        "secondary_diagnosis_id",
        "FactVisit",
        "DimSecondaryDiagnosis",
    )
    validate_foreign_key(
        fact_visit,
        dim_treatment,
        "treatment_id",
        "treatment_id",
        "FactVisit",
        "DimTreatment",
    )
    validate_foreign_key(
        fact_visit,
        dim_prescription,
        "prescription_id",
        "prescription_id",
        "FactVisit",
        "DimPrescription",
    )
    validate_foreign_key(
        fact_visit,
        dim_lab_order,
        "lab_order_id",
        "lab_order_id",
        "FactVisit",
        "DimLabOrder",
    )

    spark.stop()


if __name__ == "__main__":
    main()
