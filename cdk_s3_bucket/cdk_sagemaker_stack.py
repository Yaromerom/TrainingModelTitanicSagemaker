from aws_cdk import (
    Stack,
    CfnOutput,
)
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_sagemaker as sagemaker
from aws_cdk import aws_iam as iam
from constructs import Construct
from sagemaker import image_uris

class CdkSageMakerStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Nombre del nuevo bucket procesado
        processed_data_bucket_name = "new-processed-data-bucket-name"  # Cambia esto al nombre del nuevo bucket en us-east-2

        # Crear el bucket si es necesario
        processed_data_bucket = s3.Bucket.from_bucket_name(
            self, "UniqueProcessedDataBucket", processed_data_bucket_name
        )

        # Usar un rol de IAM existente
        sagemaker_execution_role = iam.Role.from_role_arn(
            self, "ExistingSageMakerExecutionRole",
            role_arn="arn:aws:iam::831926621099:role/SageMakerExecutionRole"
        )

        # Exportar datos para el script de boto3
        CfnOutput(self, "ProcessedDataBucketName", value=processed_data_bucket_name)
        CfnOutput(self, "SageMakerExecutionRoleArn", value=sagemaker_execution_role.role_arn)

        # Configuración del modelo (sin trabajo de entrenamiento en CDK)
        training_image_uri = image_uris.retrieve(
            framework='linear-learner',
            region='us-east-2'
        )

        model_name = "TitanicModel"
        model_data_url = f"s3://{processed_data_bucket_name}/output/TitanicTrainingJob-1734936497/output/model.tar.gz"

        model = sagemaker.CfnModel(
            self, "TitanicModel",
            execution_role_arn=sagemaker_execution_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                image=training_image_uri,
                mode="SingleModel",
                model_data_url=model_data_url
            ),
            model_name=model_name
        )

        # Configuración del endpoint
        endpoint_config_name = "TitanicEndpointConfig"
        endpoint_config = sagemaker.CfnEndpointConfig(
            self, "TitanicEndpointConfig",
            endpoint_config_name=endpoint_config_name,
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    model_name=model_name,
                    variant_name="AllTraffic",
                    initial_instance_count=1,
                    instance_type="ml.m5.large",
                    initial_variant_weight=1.0
                )
            ]
        )
        endpoint_config.node.add_dependency(model)

        endpoint_name = "TitanicEndpoint"
        endpoint = sagemaker.CfnEndpoint(
            self, "TitanicEndpoint",
            endpoint_name=endpoint_name,
            endpoint_config_name=endpoint_config_name
        )
        endpoint.node.add_dependency(endpoint_config)

        # Salidas
        CfnOutput(self, "ModelName", value=model_name)
        CfnOutput(self, "EndpointConfigName", value=endpoint_config_name)
        CfnOutput(self, "EndpointName", value=endpoint_name)
