import boto3
from sagemaker import image_uris
import time

# Configuración
region = "us-east-2"
bucket_name = "new-processed-data-bucket-name"  # Cambia por tu bucket
role_arn = "arn:aws:iam::831926621099:role/SageMakerExecutionRole"  # Cambia por tu rol
training_image = image_uris.retrieve(  # Obtén el URI del contenedor automáticamente
    framework="linear-learner",
    region=region
)
training_job_name = f"TitanicTrainingJob-{int(time.time())}"  # Nombre único
output_path = f"s3://{bucket_name}/output/"

# Cliente de SageMaker
sagemaker_client = boto3.client('sagemaker', region_name=region)

# Crear el trabajo de entrenamiento
response = sagemaker_client.create_training_job(
    TrainingJobName=training_job_name,
    AlgorithmSpecification={
        "TrainingImage": training_image,
        "TrainingInputMode": "File"
    },
    RoleArn=role_arn,
    InputDataConfig=[
        {
            "ChannelName": "train",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": f"s3://{bucket_name}/titanic-data/processed/train_processed.csv",
                    "S3DataDistributionType": "FullyReplicated"
                }
            },
            "ContentType": "text/csv"
        },
        {
            "ChannelName": "validation",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": f"s3://{bucket_name}/titanic-data/processed/test_processed.csv",
                    "S3DataDistributionType": "FullyReplicated"
                }
            },
            "ContentType": "text/csv"
        }
    ],
    OutputDataConfig={
        "S3OutputPath": output_path
    },
    ResourceConfig={
        "InstanceType": "ml.m5.large",
        "InstanceCount": 1,
        "VolumeSizeInGB": 10
    },
    StoppingCondition={
        "MaxRuntimeInSeconds": 3600
    },
    HyperParameters = {  
        "predictor_type": "multiclass_classifier",  # Tarea de clasificación multiclase
        "num_classes": "3",  # Número de clases
        "feature_dim": "4",  # Número de características (4 columnas, excluyendo la etiqueta)
        "mini_batch_size": "16",  # Dataset pequeño, permite procesar más datos por lote
        "epochs": "50",  # Entrenamiento en 50 épocas para un dataset pequeño
        "learning_rate": "0.01"  # Tasa de aprendizaje moderada para evitar saltos grandes
        # "normalize_data": "True",  # Normalización para características escaladas de forma inconsistente
        # "normalize_label": "False",  # No es necesario normalizar etiquetas en clasificación multiclase
        # "use_bias": "True",  # Incluir un sesgo para mejorar el ajuste
        # "optimizer": "adam",  # Algoritmo de optimización eficiente para modelos pequeños
        # "loss": "auto",  # SageMaker elige automáticamente la mejor pérdida para multiclase
        # "l1_regularization": "0.0001",  # Regularización ligera para evitar sobreajuste
        # "l2_regularization": "0.0001",  # Regularización ligera para evitar sobreajuste
        # "early_stopping": "True",  # Detener el entrenamiento si no hay mejora
        # "early_stopping_patience": "5"  # Paciencia para early stopping
}

)

print("Training job started. Response:")
print(response)
