import sys
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, when, round
import argparse

# -------------------------------------------------------------------------
# 1. Leer SÓLO el argumento obligatorio 'JOB_NAME' con getResolvedOptions
# -------------------------------------------------------------------------
args_job = getResolvedOptions(sys.argv, ["JOB_NAME"])

# -------------------------------------------------------------------------
# 2. Usar argparse para leer el resto de argumentos de manera opcional
# -------------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--raw_data_bucket", default="cdks3gluestack-rawdatabucket57f26c03-szdkndmvnohr")
parser.add_argument("--processed_data_bucket", default="new-processed-data-bucket-name")
parser.add_argument("--train_file_key", default="train.csv")
parser.add_argument("--test_file_key", default="test.csv")

parsed, _ = parser.parse_known_args(sys.argv[1:])

# -------------------------------------------------------------------------
# 3. Asignar los valores finales a variables
# -------------------------------------------------------------------------
job_name = args_job["JOB_NAME"]
raw_data_bucket = parsed.raw_data_bucket
processed_data_bucket = parsed.processed_data_bucket
train_file_key = parsed.train_file_key
test_file_key = parsed.test_file_key

raw_data_train_path = f"s3://{raw_data_bucket}/{train_file_key}"
raw_data_test_path = f"s3://{raw_data_bucket}/{test_file_key}"

# -------------------------------------------------------------------------
# 4. Crear SparkContext, GlueContext y Job
# -------------------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(job_name, args_job)

# -------------------------------------------------------------------------
# 5. Cargar dataframes de Spark para TRAIN y TEST
# -------------------------------------------------------------------------
df_train = spark.read.csv(raw_data_train_path, header=True, inferSchema=True)
df_test = spark.read.csv(raw_data_test_path, header=True, inferSchema=True)

# -------------------------------------------------------------------------
# 6. LIMPIEZA DE DATOS: Eliminar filas con valores nulos en columnas necesarias
# -------------------------------------------------------------------------
df_train_clean = df_train.dropna(subset=["Age", "Fare", "Embarked", "Sex"])
df_test_clean = df_test.dropna(subset=["Age", "Fare", "Embarked", "Sex"])

# -------------------------------------------------------------------------
# 7. TRANSFORMACIÓN: Categorización binaria de 'Sex'
# -------------------------------------------------------------------------
df_train_clean = df_train_clean.withColumn("Sex_Female", when(col("Sex") == "female", 1).otherwise(0))
df_train_clean = df_train_clean.withColumn("Sex_Male", when(col("Sex") == "male", 1).otherwise(0))

df_test_clean = df_test_clean.withColumn("Sex_Female", when(col("Sex") == "female", 1).otherwise(0))
df_test_clean = df_test_clean.withColumn("Sex_Male", when(col("Sex") == "male", 1).otherwise(0))

# -------------------------------------------------------------------------
# 8. CATEGORIZACIÓN DE 'Age' y 'Fare'
# -------------------------------------------------------------------------
# Segmentar 'Age' de 0 a 2
age_max_train = df_train_clean.agg({"Age": "max"}).collect()[0][0]
age_min_train = df_train_clean.agg({"Age": "min"}).collect()[0][0]
df_train_clean = df_train_clean.withColumn(
    "Age_Category", round(((col("Age") - age_min_train) / (age_max_train - age_min_train)) * 2)
)

df_test_clean = df_test_clean.withColumn(
    "Age_Category", round(((col("Age") - age_min_train) / (age_max_train - age_min_train)) * 2)
)

# Segmentar 'Fare' de 0 a 2
fare_max_train = df_train_clean.agg({"Fare": "max"}).collect()[0][0]
fare_min_train = df_train_clean.agg({"Fare": "min"}).collect()[0][0]
df_train_clean = df_train_clean.withColumn(
    "Fare_Category", round(((col("Fare") - fare_min_train) / (fare_max_train - fare_min_train)) * 2)
)

df_test_clean = df_test_clean.withColumn(
    "Fare_Category", round(((col("Fare") - fare_min_train) / (fare_max_train - fare_min_train)) * 2)
)

# -------------------------------------------------------------------------
# 9. FILTRAR COLUMNAS REQUERIDAS: 'Survived' al final
# -------------------------------------------------------------------------
columns_to_keep_train = ["Sex_Female", "Sex_Male", "Age_Category", "Fare_Category", "Survived"]
df_train_output = df_train_clean.select([col for col in columns_to_keep_train if col in df_train_clean.columns])

columns_to_keep_test = ["Sex_Female", "Sex_Male", "Age_Category", "Fare_Category", "Survived"]
df_test_output = df_test_clean.select([col for col in columns_to_keep_test if col in df_test_clean.columns])

# -------------------------------------------------------------------------
# 10. Guardar resultados procesados en S3 sin encabezados con nombres correctos
# -------------------------------------------------------------------------
import uuid

temp_train_path = f"s3://{processed_data_bucket}/titanic-data/processed/temp_train_{uuid.uuid4()}"
temp_test_path = f"s3://{processed_data_bucket}/titanic-data/processed/temp_test_{uuid.uuid4()}"

df_train_output.coalesce(1).write.csv(temp_train_path, header=False, mode="overwrite")
df_test_output.coalesce(1).write.csv(temp_test_path, header=False, mode="overwrite")

# Renombrar archivos a los nombres correctos
s3 = boto3.client("s3", region_name="us-east-2")

# Procesar archivo de entrenamiento
train_objects = s3.list_objects_v2(Bucket=processed_data_bucket, Prefix="titanic-data/processed/temp_train_")
for obj in train_objects.get("Contents", []):
    if obj["Key"].endswith(".csv"):
        s3.copy_object(
            Bucket=processed_data_bucket,
            CopySource={"Bucket": processed_data_bucket, "Key": obj["Key"]},
            Key="titanic-data/processed/train_processed.csv"
        )
        s3.delete_object(Bucket=processed_data_bucket, Key=obj["Key"])

# Procesar archivo de prueba
test_objects = s3.list_objects_v2(Bucket=processed_data_bucket, Prefix="titanic-data/processed/temp_test_")
for obj in test_objects.get("Contents", []):
    if obj["Key"].endswith(".csv"):
        s3.copy_object(
            Bucket=processed_data_bucket,
            CopySource={"Bucket": processed_data_bucket, "Key": obj["Key"]},
            Key="titanic-data/processed/test_processed.csv"
        )
        s3.delete_object(Bucket=processed_data_bucket, Key=obj["Key"])

# -------------------------------------------------------------------------
# 11. Finalizar el Job
# -------------------------------------------------------------------------
job.commit()
