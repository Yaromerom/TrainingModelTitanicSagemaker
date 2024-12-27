#!/usr/bin/env python3

from aws_cdk import App
from cdk_s3_bucket.cdk_s3_bucket_stack import CdkS3BucketStack
from cdk_s3_bucket.cdk_glue_stack import CdkS3GlueStack
from cdk_s3_bucket.cdk_sagemaker_stack import CdkSageMakerStack  # <--- nuevo import

app = App()

# 1. Crear stack para los buckets
s3_stack = CdkS3BucketStack(app, "CdkS3BucketStack")

# 2. Crear stack para Glue
glue_stack = CdkS3GlueStack(app, "CdkS3GlueStack")

# 3. Crear stack para SageMaker
sagemaker_stack = CdkSageMakerStack(app, "CdkSageMakerStack")

# Opcional: si el Sagemaker Stack depende del output del Glue Stack 
# (por ejemplo, tener la ruta de S3 generada en GlueStack), 
# podrías hacer algo como: 
#
# sagemaker_stack.add_dependency(glue_stack)
#
# Y/o pasar parámetros (como bucket name) al constructor del SageMaker stack

# Finalmente, sintetizar la aplicación
app.synth()
