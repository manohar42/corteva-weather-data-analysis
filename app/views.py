from flask import Blueprint, request, jsonify
from sqlalchemy import text
from .db import db

api_bp = Blueprint("api", __name__)

def paginate():
    page = max(int(request.args.get("page", 1)), 1)
    per = min(max(int(request.args.get("per", 50)), 1), 500)
    return per, (page - 1) * per

@api_bp.route("/weather/")
def weather():
    per, offset = paginate()
    sql = text("""
        SELECT id, station_id, date, max_temp, min_temp, precipitation
        FROM weather_data
        ORDER BY date, station_id
        LIMIT :limit OFFSET :offset
    """)
    rows = db.session.execute(sql, {"limit": per, "offset": offset}).mappings().all()
    return jsonify([dict(r) for r in rows])

@api_bp.route("/weather/stats")
def stats():
    per, offset = paginate()
    sql = text("""
        SELECT id, station_id, year,
               avg_maximum_temperature, avg_minimum_temperature, total_precipitation
        FROM statistics_data
        ORDER BY year, station_id
        LIMIT :limit OFFSET :offset
    """)
    rows = db.session.execute(sql, {"limit": per, "offset": offset}).mappings().all()
    return jsonify([dict(r) for r in rows])
