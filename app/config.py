import os
class Config:

    SQLALCHEMY_DATABASE_URI = os.getenv("DATABASE_URL", "postgresql://user:password@localhost/corteva_weather")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
