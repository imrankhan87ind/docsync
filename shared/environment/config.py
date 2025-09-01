import os
from dataclasses import dataclass
from typing import Optional

# Utility to read env var and convert to bool
def _get_bool_env_var(name: str, default: str = 'false') -> bool:
    return os.environ.get(name, default).lower() in ('true', '1', 't')

@dataclass(frozen=True)
class MinioConfig:
    """Typed configuration for Minio client."""
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool

@dataclass(frozen=True)
class MongoConfig:
    """Typed configuration for MongoDB client."""
    host: str
    port: int
    user: str
    password: str
    dbname: str

@dataclass(frozen=True)
class RabbitMQConfig:
    """Typed configuration for RabbitMQ client."""
    host: str
    port: int
    user: str
    password: str

class AppConfig:
    """
    A centralized, type-safe configuration class that loads settings from
    environment variables.

    It uses a property-based access pattern. The internal configuration objects
    (_minio, _mongo, etc.) are loaded once and are Optional. Public properties
    (minio, mongo, etc.) provide non-optional access, raising a RuntimeError
    if the configuration was not loaded, ensuring fail-fast behavior and
    type safety for consumers.
    """
    def __init__(self):
        # Internal storage for configurations, loaded once.
        self._minio: Optional[MinioConfig] = self._load_minio_config()
        self._mongo: Optional[MongoConfig] = self._load_mongo_config()
        self._rabbitmq: Optional[RabbitMQConfig] = self._load_rabbitmq_config()
        self.grpc_port: int = int(os.environ.get("GRPC_PORT", "50051"))

    @property
    def minio(self) -> MinioConfig:
        """Provides access to the MinioConfig. Raises RuntimeError if not configured."""
        if self._minio is None:
            raise RuntimeError(
                "Minio configuration is not loaded. "
                "Ensure MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY are set."
            )
        return self._minio

    @property
    def mongo(self) -> MongoConfig:
        """Provides access to the MongoConfig. Raises RuntimeError if not configured."""
        if self._mongo is None:
            raise RuntimeError(
                "MongoDB configuration is not loaded. "
                "Ensure MONGO_HOST, MONGO_USER, MONGO_PASSWORD, and MONGO_DBNAME are set."
            )
        return self._mongo

    @property
    def rabbitmq(self) -> RabbitMQConfig:
        """Provides access to the RabbitMQConfig. Raises RuntimeError if not configured."""
        if self._rabbitmq is None:
            raise RuntimeError(
                "RabbitMQ configuration is not loaded. "
                "Ensure RABBITMQ_HOST, RABBITMQ_USER, and RABBITMQ_PASSWORD are set."
            )
        return self._rabbitmq

    def _load_minio_config(self) -> Optional[MinioConfig]:
        """Loads Minio config from environment variables."""
        endpoint = os.environ.get("MINIO_ENDPOINT")
        access_key = os.environ.get("MINIO_ACCESS_KEY")
        secret_key = os.environ.get("MINIO_SECRET_KEY")

        if endpoint and access_key and secret_key:
            return MinioConfig(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=_get_bool_env_var("MINIO_SECURE", "false")
            )
        return None

    def _load_mongo_config(self) -> Optional[MongoConfig]:
        """Loads MongoDB config from environment variables."""
        host = os.environ.get("MONGO_HOST")
        user = os.environ.get("MONGO_USER")
        password = os.environ.get("MONGO_PASSWORD")
        dbname = os.environ.get("MONGO_DBNAME")

        if host and user and password and dbname:
            return MongoConfig(
                host=host,
                port=int(os.environ.get("MONGO_PORT", "27017")),
                user=user,
                password=password,
                dbname=dbname
            )
        return None

    def _load_rabbitmq_config(self) -> Optional[RabbitMQConfig]:
        """Loads RabbitMQ config from environment variables."""
        host = os.environ.get("RABBITMQ_HOST")
        user = os.environ.get("RABBITMQ_USER")
        password = os.environ.get("RABBITMQ_PASSWORD")

        if host and user and password:
            return RabbitMQConfig(
                host=host,
                port=int(os.environ.get("RABBITMQ_PORT", "5672")),
                user=user,
                password=password
            )
        return None

    def assert_all(self) -> None:
        """
        Asserts that all required configurations are loaded by accessing them.
        This will trigger the property checks and raise a RuntimeError if any
        are missing.
        """
        _ = self.minio
        _ = self.mongo
        _ = self.rabbitmq

# Singleton instance of the application configuration.
app_config = AppConfig()
