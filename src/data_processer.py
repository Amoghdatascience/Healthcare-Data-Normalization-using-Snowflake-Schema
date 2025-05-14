import os
import shutil
import glob
from pyspark.sql.functions import col, when, year, md5, concat_ws


def load_raw_data(spark, input_path):
    print(f"Loading data from {input_path}...")
    return spark.read.option("header", "true").csv(input_path)


def clean_output_folder(output_path):
    print(" Cleaning old output files...")
    for entry in os.listdir(output_path):
        full_path = os.path.join(output_path, entry)
        if os.path.isfile(full_path):
            os.remove(full_path)
        elif os.path.isdir(full_path):
            shutil.rmtree(full_path)
    print(" Output folder is clean.")


def validate_primary_key(df, key_col, table_name):
    total = df.count()
    unique = df.select(key_col).distinct().count()
    if total != unique:
        print(
            f"❌ PK Validation FAILED for {table_name}.{key_col} – Duplicate keys found."
        )
    else:
        print(f"✅ PK Validation PASSED for {table_name}.{key_col}")


def validate_foreign_key(fact_df, dim_df, fk_col, pk_col, fk_table, pk_table):
    unmatched = fact_df.select(fk_col).subtract(dim_df.select(pk_col))
    count = unmatched.filter(col(fk_col).isNotNull()).count()
    if count > 0:
        print(f"❌ FK Validation FAILED: {count} missing {fk_col} values in {pk_table}")
        unmatched.show(5)
    else:
        print(
            f"✅ FK Validation PASSED: All {fk_col} in {fk_table} match {pk_col} in {pk_table}"
        )


def normalize_dim_patient(df):
    print("Normalizing DimPatient...")
    return (
        df.select(
            "patient_id",
            "patient_first_name",
            "patient_last_name",
            "patient_date_of_birth",
            "patient_gender",
            "patient_address_line1",
            "patient_address_line2",
            "patient_city",
            "patient_state",
            "patient_zip",
            "patient_phone",
            "patient_email",
            "visit_datetime",
        )
        .withColumn(
            "patient_status",
            when(
                (col("visit_datetime").isNotNull()) & (year("visit_datetime") > 2021),
                "Active",
            ).otherwise("Inactive"),
        )
        .drop("visit_datetime")
        .dropDuplicates(["patient_id"])
    )


def normalize_dim_insurance(df):
    print("Normalizing DimInsurance...")
    return df.select(
        "insurance_id",
        "patient_id",
        "insurance_payer_name",
        "insurance_policy_number",
        "insurance_group_number",
        "insurance_plan_type",
    ).dropDuplicates(["insurance_id"])


def normalize_dim_billing(df):
    print("Normalizing DimBilling...")
    return df.select(
        "billing_id",
        "insurance_id",
        "billing_total_charge",
        "billing_amount_paid",
        "billing_date",
        "billing_payment_status",
    ).dropDuplicates(["billing_id"])


def normalize_dim_provider(df):
    print("Normalizing DimProvider...")
    return (
        df.select("doctor_name", "doctor_title", "doctor_department")
        .dropDuplicates()
        .withColumn(
            "provider_id",
            md5(concat_ws("-", "doctor_name", "doctor_title", "doctor_department")),
        )
        .select("provider_id", "doctor_name", "doctor_title", "doctor_department")
    )


def normalize_dim_location(df):
    print("Normalizing DimLocation...")
    return (
        df.select("clinic_name", "room_number")
        .dropDuplicates()
        .withColumn("location_id", md5(concat_ws("-", "clinic_name", "room_number")))
        .select("location_id", "clinic_name", "room_number")
    )


def normalize_dim_primary_diagnosis(df):
    print("Normalizing DimPrimaryDiagnosis...")
    return (
        df.select("primary_diagnosis_code", "primary_diagnosis_desc")
        .dropDuplicates()
        .withColumn(
            "primary_diagnosis_id",
            md5(concat_ws("-", "primary_diagnosis_code", "primary_diagnosis_desc")),
        )
        .select(
            "primary_diagnosis_id", "primary_diagnosis_code", "primary_diagnosis_desc"
        )
    )


def normalize_dim_secondary_diagnosis(df):
    print("Normalizing DimSecondaryDiagnosis...")
    return (
        df.filter(
            col("secondary_diagnosis_code").isNotNull()
            & col("secondary_diagnosis_desc").isNotNull()
        )
        .select("secondary_diagnosis_code", "secondary_diagnosis_desc")
        .dropDuplicates()
        .withColumn(
            "secondary_diagnosis_id",
            md5(concat_ws("-", "secondary_diagnosis_code", "secondary_diagnosis_desc")),
        )
        .select(
            "secondary_diagnosis_id",
            "secondary_diagnosis_code",
            "secondary_diagnosis_desc",
        )
    )


def normalize_dim_treatment(df):
    print("Normalizing DimTreatment...")
    return (
        df.select("treatment_code", "treatment_desc")
        .dropDuplicates()
        .withColumn(
            "treatment_id", md5(concat_ws("-", "treatment_code", "treatment_desc"))
        )
        .select("treatment_id", "treatment_code", "treatment_desc")
    )


def normalize_dim_prescription(df):
    print("Normalizing DimPrescription...")
    return (
        df.filter(col("prescription_id").isNotNull())
        .select(
            "prescription_id",
            "prescription_drug_name",
            "prescription_dosage",
            "prescription_frequency",
            "prescription_duration_days",
        )
        .dropDuplicates(["prescription_id"])
    )


def normalize_dim_lab_order(df):
    print("Normalizing DimLabOrder...")
    return df.select(
        "lab_order_id",
        "lab_test_code",
        "lab_name",
        "lab_result_value",
        "lab_result_units",
        "lab_result_date",
    ).dropDuplicates(["lab_order_id"])


def normalize_fact_visit(
    df, dim_provider, dim_location, dim_primary_dx, dim_secondary_dx, dim_treatment
):
    print("Normalizing FactVisit...")
    df = df.join(
        dim_provider,
        on=["doctor_name", "doctor_title", "doctor_department"],
        how="left",
    )
    df = df.join(dim_location, on=["clinic_name", "room_number"], how="left")
    df = df.join(
        dim_primary_dx,
        on=["primary_diagnosis_code", "primary_diagnosis_desc"],
        how="left",
    )
    df = df.join(
        dim_secondary_dx,
        on=["secondary_diagnosis_code", "secondary_diagnosis_desc"],
        how="left",
    )
    df = df.join(dim_treatment, on=["treatment_code", "treatment_desc"], how="left")

    return df.select(
        "visit_id",
        "patient_id",
        "insurance_id",
        "billing_id",
        "provider_id",
        "location_id",
        "primary_diagnosis_id",
        "secondary_diagnosis_id",
        "treatment_id",
        "prescription_id",
        "lab_order_id",
        "visit_datetime",
        "visit_type",
    ).dropDuplicates(["visit_id"])


def save_to_csv(df, output_path, table_name):
    print(f"Saving {table_name} to flat CSV...")
    final_file = f"{output_path}{table_name}.csv"

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        output_path + "_tmp"
    )

    part_file = glob.glob(f"{output_path}_tmp/part-*.csv")[0]
    shutil.move(part_file, final_file)
    shutil.rmtree(output_path + "_tmp")

    print(f" Written flat file: {final_file}")
