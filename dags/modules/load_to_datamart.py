import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
import os


# Membuat SparkSession
spark = SparkSession.builder \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.0") \
    .master("local") \
    .appName("PySpark_Postgres").getOrCreate()


# Fungsi untuk Extract dan Transform Data menggunakan Spark
def extract_transform_spark():
    # Membaca tabel dari PostgreSQL dan membuat Temp View
    tables = {
        "dim_date": "kelompok1_dwh.dim_date",
        "dim_candidate": "kelompok1_dwh.dim_candidate",
        "dim_employee": "kelompok1_dwh.dim_employee",
        "dim_review_period": "kelompok1_dwh.dim_review_period",
        "dim_training_program": "kelompok1_dwh.dim_training_program",
        "fact_payroll": "kelompok1_dwh.fact_payroll",
        "fact_performance": "kelompok1_dwh.fact_performance",
        "fact_recruitment": "kelompok1_dwh.fact_recruitment",
        "fact_training": "kelompok1_dwh.fact_training",
    }

    for view_name, db_table in tables.items():
        df = spark.read.format("jdbc") \
            .option("url", "jdbc:postgresql://34.56.65.122:5432/ftde03") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", db_table) \
            .option("user", "ftde03") \
            .option("password", "ftde03!@#").load()
        df.createOrReplaceTempView(view_name)

    # Transformasi Recruitment Data
    df_recruitment = spark.sql('''
    SELECT
        fact_recruitment.recruitment_key,
        fact_recruitment.candidate_key,
        dim_candidate.name,
        dim_candidate.gender,
        dim_candidate.age,
        dim_candidate.position,
        fact_recruitment.application_date_key,
        fact_recruitment.interview_date_key,
        CAST(app_date.date_day AS DATE) AS application_date,
        CAST(app_date.year AS INT) AS application_year,
        CAST(app_date.quarter AS INT) AS application_quarter,
        CAST(app_date.month AS INT) AS application_month,
        CAST(app_date.day AS INT) AS application_day,
        CAST(int_date.date_day AS DATE) AS interview_date,
        CAST(int_date.year AS INT) AS interview_year,
        CAST(int_date.quarter AS INT) AS interview_quarter,
        CAST(int_date.month AS INT) AS interview_month,
        CAST(int_date.day AS INT) AS interview_day,
        fact_recruitment.offerstatus
    FROM fact_recruitment
    LEFT JOIN dim_candidate ON fact_recruitment.candidate_key = dim_candidate.candidate_key
    LEFT JOIN dim_date AS app_date ON fact_recruitment.application_date_key = app_date.date_key
    LEFT JOIN dim_date AS int_date ON fact_recruitment.interview_date_key = int_date.date_key
    ORDER BY fact_recruitment.recruitment_key ASC
    ''')

    # Transformasi Training Data
    df_training = spark.sql('''
    SELECT
        fact_training.training_key,
        fact_training.employee_key,
        dim_employee.name,
        dim_employee.gender,
        dim_employee.age,
        dim_employee.department,
        dim_employee.position,
        fact_training.training_program_key,
        dim_training_program.trainingprogram AS training_program,
        fact_training.start_date_key,
        fact_training.end_date_key,
        CAST(start_date.date_day AS DATE) AS start_date,
        CAST(start_date.year AS INT) AS start_year,
        CAST(start_date.quarter AS INT) AS start_quarter,
        CAST(start_date.month AS INT) AS start_month,
        CAST(start_date.day AS INT) AS start_day,
        CAST(end_date.date_day AS DATE) AS end_date,
        CAST(end_date.year AS INT) AS end_year,
        CAST(end_date.quarter AS INT) AS end_quarter,
        CAST(end_date.month AS INT) AS end_month,
        CAST(end_date.day AS INT) AS end_day,
        fact_training.status
    FROM fact_training
    LEFT JOIN dim_employee ON fact_training.employee_key = dim_employee.employee_key
    LEFT JOIN dim_training_program ON fact_training.training_program_key = dim_training_program.training_program_key
    LEFT JOIN dim_date AS start_date ON fact_training.start_date_key = start_date.date_key
    LEFT JOIN dim_date AS end_date ON fact_training.end_date_key = end_date.date_key
    ORDER BY fact_training.training_key ASC
    ''')

    # Transformasi Payroll Data
    df_payroll = spark.sql('''
    SELECT
        fact_payroll.payroll_key,
        fact_payroll.employee_key,
        dim_employee.employeeid AS employee_id,
        dim_employee.name,
        dim_employee.gender,
        dim_employee.age,
        dim_employee.department,
        dim_employee.position,
        fact_payroll.date_key,
        CAST(date.date_day AS DATE) AS date,
        CAST(date.year AS INT) AS year,
        CAST(date.quarter AS INT) AS quarter,
        CAST(date.month AS INT) AS month,
        CAST(date.day AS INT) AS day,
        fact_payroll.salary,
        fact_payroll.overtimepay
    FROM fact_payroll
    LEFT JOIN dim_employee ON fact_payroll.employee_key = dim_employee.employee_key
    LEFT JOIN dim_date AS date ON fact_payroll.date_key = date.date_key
    ORDER BY fact_payroll.payroll_key ASC
    ''')

    # Transformasi Performance Data
    df_performance = spark.sql('''
    SELECT
        fact_performance.performance_key,
        fact_performance.employee_key,
        dim_employee.employeeid AS employee_id,
        dim_employee.name,
        dim_employee.gender,
        dim_employee.age,
        dim_employee.department,
        dim_employee.position,
        fact_performance.review_period_key,
        dim_review_period.reviewperiod AS review_period,
        fact_performance.rating
    FROM fact_performance
    LEFT JOIN dim_employee ON fact_performance.employee_key = dim_employee.employee_key
    LEFT JOIN dim_review_period ON fact_performance.review_period_key = dim_review_period.review_period_key
    ORDER BY fact_performance.performance_key ASC
    ''')

    # Menyimpan hasil transformasi ke dalam Parquet
    df_recruitment.write.mode('overwrite') \
        .partitionBy('application_year', 'application_month') \
        .option('compression', 'snappy') \
        .option('partitionOverwriteMode', 'dynamic') \
        .save('data_recruitment_result')

    df_training.write.mode('overwrite') \
        .partitionBy('start_year', 'start_month') \
        .option('compression', 'snappy') \
        .option('partitionOverwriteMode', 'dynamic') \
        .save('data_training_result')

    df_payroll.write.mode('overwrite') \
        .partitionBy('year', 'month') \
        .option('compression', 'snappy') \
        .option('partitionOverwriteMode', 'dynamic') \
        .save('data_payroll_result')

    df_performance.write.mode('overwrite') \
        .partitionBy('review_period') \
        .option('compression', 'snappy') \
        .option('partitionOverwriteMode', 'dynamic') \
        .save('data_performance_result')


# Fungsi untuk Memuat Data ke Google Spreadsheet
def load_to_marts():
    def save_to_spreadsheet(spark_df, spreadsheet_name):
        # Lokasi credentials.json dalam container Docker
        credentials_path = "/usr/local/airflow/dags/modules/credentials/credentials.json"  
        
        pandas_df = spark_df.toPandas()
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = Credentials.from_service_account_file(credentials_path, scopes=scope)
        gc = gspread.authorize(creds)

        try:
            sh = gc.open(spreadsheet_name)
        except gspread.exceptions.SpreadsheetNotFound:
            sh = gc.create(spreadsheet_name)

        worksheet = sh.sheet1
        set_with_dataframe(worksheet, pandas_df)

        print(f"Data berhasil diunggah ke Google Spreadsheet: {spreadsheet_name}")

    # Membaca data yang sudah disimpan dalam format Parquet
    df_spark_recruitment = spark.read.parquet('data_recruitment_result')
    save_to_spreadsheet(df_spark_recruitment, "recruitment")

    df_spark_training = spark.read.parquet('data_training_result')
    save_to_spreadsheet(df_spark_training, "training")

    df_spark_payroll = spark.read.parquet('data_payroll_result')
    save_to_spreadsheet(df_spark_payroll, "payroll")

    df_spark_performance = spark.read.parquet('data_performance_result')
    save_to_spreadsheet(df_spark_performance, "performance")

    print("Semua data berhasil diunggah ke Google Spreadsheet.")


# Eksekusi proses ETL
if __name__ == "__main__":
    extract_transform_spark
