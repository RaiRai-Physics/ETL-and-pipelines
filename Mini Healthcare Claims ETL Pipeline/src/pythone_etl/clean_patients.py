import csv
from pathlib import Path


RAW_FILE = Path("data/raw/patients.csv")
CLEAN_FILE = Path("data/clean/patients_clean.csv")
REPORT_FILE = Path("reports/data_quality_report.txt")


def read_csv(file_path):
    with open(file_path, mode="r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return list(reader)


def standardize_gender(gender):
    if gender is None or gender.strip() == "":
        return "NA"

    gender = gender.strip().upper()

    if gender in ["M", "MALE"]:
        return "Male"
    elif gender in ["F", "FEMALE"]:
        return "Female"
    else:
        return "NA"


def clean_patients(rows):
    cleaned_rows = []
    seen_patient_ids = set()
    duplicate_count = 0

    for row in rows:
        patient_id = row["patient_id"].strip()

        if patient_id in seen_patient_ids:
            duplicate_count += 1
            continue

        seen_patient_ids.add(patient_id)

        cleaned_row = {
            "patient_id": patient_id,
            "name": row["name"].strip() if row["name"].strip() else "NA",
            "age": row["age"].strip() if row["age"].strip() else "NA",
            "gender": standardize_gender(row["gender"]),
            "city": row["city"].strip() if row["city"].strip() else "NA",
            "disease": row["disease"].strip() if row["disease"].strip() else "NA",
        }

        cleaned_rows.append(cleaned_row)

    return cleaned_rows, duplicate_count


def write_csv(file_path, rows):
    file_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["patient_id", "name", "age", "gender", "city", "disease"]

    with open(file_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_report(total_rows, cleaned_rows, duplicate_count):
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)

    missing_name_count = sum(1 for row in cleaned_rows if row["name"] == "NA")
    missing_age_count = sum(1 for row in cleaned_rows if row["age"] == "NA")
    missing_gender_count = sum(1 for row in cleaned_rows if row["gender"] == "NA")

    with open(REPORT_FILE, mode="w", encoding="utf-8") as file:
        file.write("Data Quality Report\n")
        file.write("===================\n")
        file.write(f"Raw row count: {total_rows}\n")
        file.write(f"Clean row count: {len(cleaned_rows)}\n")
        file.write(f"Duplicate rows removed: {duplicate_count}\n")
        file.write(f"Missing names replaced: {missing_name_count}\n")
        file.write(f"Missing ages replaced: {missing_age_count}\n")
        file.write(f"Missing genders replaced: {missing_gender_count}\n")


def main():
    rows = read_csv(RAW_FILE)
    cleaned_rows, duplicate_count = clean_patients(rows)

    write_csv(CLEAN_FILE, cleaned_rows)
    write_report(len(rows), cleaned_rows, duplicate_count)

    print("ETL completed successfully.")
    print(f"Clean file written to: {CLEAN_FILE}")
    print(f"Report written to: {REPORT_FILE}")


if __name__ == "__main__":
    main()
