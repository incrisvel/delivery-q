from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    rabbitmq_user: str
    rabbitmq_pass: str
    rabbitmq_host: str
    rabbitmq_port: str
    rabbitmq_vhost: str

    class Config:
        env_file = '.env'

settings = Settings()