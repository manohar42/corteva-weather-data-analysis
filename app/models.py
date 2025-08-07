from .db import db

class WeatherData(db.Model):
    __tablename__ = "weather_data"
    id = db.Column(db.Integer, primary_key=True)
    station_id = db.Column(db.String(11), nullable=False)
    date = db.Column(db.Date, nullable=False)
    max_temp = db.Column(db.Numeric(5, 2))
    min_temp = db.Column(db.Numeric(5, 2))
    precipitation = db.Column(db.Numeric(6, 2))

class Statistics(db.Model):
    __tablename__ = "statistics_data"
    stat_id = db.Column(db.Integer, primary_key=True)
    station_id = db.Column(db.String(11), nullable=False)
    year = db.Column(db.Integer, nullable=False)
    avg_maximum_temperature = db.Column(db.Numeric(5, 2), nullable=False)
    avg_minimum_temperature = db.Column(db.Numeric(5, 2), nullable=False)
    total_precipitation = db.Column(db.Numeric(6, 2), nullable=False)
