from aws_cdk import Stack, RemovalPolicy
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_iam as iam
from constructs import Construct

class CdkS3BucketStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Crear un bucket S3
        bucket = s3.Bucket(self, "MyFirstBucket",
                           versioned=True,
                           removal_policy=RemovalPolicy.DESTROY,
                           auto_delete_objects=True,  # Solo en entornos de desarrollo
                           block_public_access=s3.BlockPublicAccess.BLOCK_ALL,  # Bloquear acceso público
                           encryption=s3.BucketEncryption.S3_MANAGED  # Habilitar cifrado gestionado por AWS
                           )

        # Referenciar usuarios existentes en IAM
        admin_user = iam.User.from_user_name(self, "ExistingAdminUser", "max.admin")
        s3_access_user = iam.User.from_user_name(self, "ExistingS3AccessUser", "max.s3access")

        # Permitir acceso completo al usuario admin
        bucket.grant_read_write(admin_user)

        # Permitir solo acceso de lectura al usuario s3access
        bucket.grant_read(s3_access_user)

        # (Opcional) Agregar políticas personalizadas a nivel de recurso
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{bucket.bucket_arn}/*"],
                principals=[iam.ArnPrincipal(s3_access_user.user_arn)],
                effect=iam.Effect.ALLOW
            )
        )

        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:*"],
                resources=[bucket.bucket_arn, f"{bucket.bucket_arn}/*"],
                principals=[iam.ArnPrincipal(admin_user.user_arn)],
                effect=iam.Effect.ALLOW
            )
        )

        # Opcional: Mostrar detalles en la consola
        self.output_bucket_details(bucket)

    def output_bucket_details(self, bucket: s3.Bucket) -> None:
        from aws_cdk import CfnOutput
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
