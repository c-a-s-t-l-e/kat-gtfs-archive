import os
import requests
import pandas as pd
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2

# KAT GTFS-RT Public Endpoints (Syncromatics)
VEHICLE_POS_URL = "https://Knoxville.Syncromatics.com/GTFS-rt/VehiclePositions"
TRIP_UPDATES_URL = "https://Knoxville.Syncromatics.com/GTFS-rt/TripUpdates"


def fetch_protobuf(url: str) -> gtfs_realtime_pb2.FeedMessage:
    response = requests.get(url, timeout=12)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def collect_vehicle_positions(fetch_time: datetime, today_str: str):
    """Parses vehicle GPS, bearing, speed, and active trip assignments."""
    try:
        feed = fetch_protobuf(VEHICLE_POS_URL)
    except Exception as e:
        print(f"[Error] Failed to fetch Vehicle Positions: {e}")
        return

    records = []
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            timestamp = (
                datetime.fromtimestamp(v.timestamp, tz=timezone.utc)
                if v.timestamp
                else fetch_time
            )

            records.append({
                "fetch_timestamp": fetch_time,
                "msg_timestamp": timestamp,
                "vehicle_id": v.vehicle.id if v.HasField("vehicle") else None,
                "vehicle_label": v.vehicle.label if v.HasField("vehicle") else None,
                "trip_id": v.trip.trip_id if v.HasField("trip") else None,
                "route_id": v.trip.route_id if v.HasField("trip") else None,
                "latitude": v.position.latitude if v.HasField("position") else None,
                "longitude": v.position.longitude if v.HasField("position") else None,
                "bearing": v.position.bearing if v.HasField("position") else None,
                "speed_mph": (v.position.speed * 2.23694)
                if v.HasField("position") and v.position.speed
                else None,
                "current_stop_sequence": v.current_stop_sequence,
                "current_status": v.current_status,
            })

    if records:
        save_to_parquet(
            records,
            folder="data/vehicles",
            filename=f"kat_vehicles_{today_str}.parquet",
            dedup_cols=["msg_timestamp", "vehicle_id"],
        )


def collect_trip_delays(fetch_time: datetime, today_str: str):
    """Parses stop-by-stop delay seconds and classifies schedule adherence."""
    try:
        feed = fetch_protobuf(TRIP_UPDATES_URL)
    except Exception as e:
        print(f"[Error] Failed to fetch Trip Updates: {e}")
        return

    records = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            tu = entity.trip_update
            trip_id = tu.trip.trip_id
            route_id = tu.trip.route_id
            vehicle_id = tu.vehicle.id if tu.HasField("vehicle") else None

            for stu in tu.stop_time_update:
                delay_sec = None
                if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                    delay_sec = stu.arrival.delay
                elif stu.HasField("departure") and stu.departure.HasField("delay"):
                    delay_sec = stu.departure.delay

                if delay_sec is not None:
                    # Classify status based on industry-standard window (±60 seconds)
                    if delay_sec > 60:
                        status = "LATE"
                    elif delay_sec < -60:
                        status = "EARLY"
                    else:
                        status = "ON_TIME"

                    records.append({
                        "fetch_timestamp": fetch_time,
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "vehicle_id": vehicle_id,
                        "stop_id": stu.stop_id,
                        "stop_sequence": stu.stop_sequence,
                        "delay_seconds": delay_sec,
                        "delay_minutes": round(delay_sec / 60.0, 2),
                        "status": status,
                    })

    if records:
        save_to_parquet(
            records,
            folder="data/delays",
            filename=f"kat_delays_{today_str}.parquet",
            dedup_cols=["fetch_timestamp", "trip_id", "stop_id"],
        )


def save_to_parquet(records: list, folder: str, filename: str, dedup_cols: list):
    """Helper function to load existing Parquet, merge, deduplicate, and overwrite."""
    os.makedirs(folder, exist_ok=True)
    out_path = os.path.join(folder, filename)
    new_df = pd.DataFrame(records)

    if os.path.exists(out_path):
        existing_df = pd.read_parquet(out_path)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df.drop_duplicates(subset=dedup_cols, inplace=True)
        combined_df.to_parquet(out_path, index=False, compression="snappy")
    else:
        new_df.to_parquet(out_path, index=False, compression="snappy")

    print(f"[{folder}] Recorded {len(records)} entries -> {out_path}")


if __name__ == "__main__":
    now = datetime.now(timezone.utc)
    date_key = now.strftime("%Y_%m_%d")

    print(f"Running KAT GTFS-RT Extractor at {now.isoformat()}...")
    collect_vehicle_positions(now, date_key)
    collect_trip_delays(now, date_key)
