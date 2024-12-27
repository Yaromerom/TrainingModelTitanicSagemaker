from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
    CfnOutput
)
from constructs import Construct

class CdkS3GlueStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Crear el bucket S3 para datos crudos
        raw_data_bucket = s3.Bucket(self, "RawDataBucket",
                                    versioned=True,
                                    removal_policy=RemovalPolicy.DESTROY,
                                    auto_delete_objects=True,
                                    block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                    encryption=s3.BucketEncryption.S3_MANAGED)

        # Crear el bucket S3 para datos procesados
        processed_data_bucket = s3.Bucket(self, "ProcessedDataBucket",
                                          versioned=True,
                                          removal_policy=RemovalPolicy.DESTROY,
                                          auto_delete_objects=True,
                                          block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                          encryption=s3.BucketEncryption.S3_MANAGED)

        # Referenciar el usuario IAM existente con política AdministratorAccess
        admin_policy = iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess")
        admin_user = iam.User.from_user_name(self, "ExistingAdminUser", "max.admin")

        # Crear un rol de servicio para Glue
        glue_service_role = iam.Role(self, "GlueServiceRole",
                                     assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
                                     managed_policies=[admin_policy])

        # Otorgar permisos de lectura/escritura a los buckets
        raw_data_bucket.grant_read_write(glue_service_role)
        processed_data_bucket.grant_read_write(glue_service_role)

        # Ruta del script de Glue
        script_location = "s3://cdks3bucketstack-myfirstbucketb8884501-gatjhberrhey/glue-job-script.py"  # Cambiar por tu ruta S3

        # Crear un trabajo de AWS Glue usando el rol de servicio
        glue_job = glue.CfnJob(self, "GlueJob",
                               name="TitanicDataPreprocessing",
                               role=glue_service_role.role_arn,  # Usar el ARN del rol de servicio
                               command={
                                   "name": "glueetl",
                                   "scriptLocation": script_location,
                                   "pythonVersion": "3"
                               },
                               default_arguments={
                                   "--additional-python-modules": "pandas",
                                   "--TempDir": f"s3://{processed_data_bucket.bucket_name}/temp/",
                                   "--job-bookmark-option": "job-bookmark-enable"
                               },
                               max_capacity=2,
                               glue_version="3.0")

        # Salidas para facilitar acceso
        CfnOutput(self, "RawDataBucketName", value=raw_data_bucket.bucket_name)
        CfnOutput(self, "ProcessedDataBucketName", value=processed_data_bucket.bucket_name)
        CfnOutput(self, "GlueJobName", value=glue_job.name)
