import json
import os
import requests
from datetime import datetime, timezone
from google.transit import gtfs_realtime_pb2

# KAT GTFS-RT Endpoints
VEHICLE_POS_URL = "https://Knoxville.Syncromatics.com/GTFS-rt/VehiclePositions"
TRIP_UPDATES_URL = "https://Knoxville.Syncromatics.com/GTFS-rt/TripUpdates"
ALERTS_URL = "https://Knoxville.Syncromatics.com/GTFS-rt/Alerts"


def fetch_protobuf(url: str) -> gtfs_realtime_pb2.FeedMessage:
    response = requests.get(url, timeout=12)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


def save_json_snapshot(data: list | dict, filepath: str):
    """Saves formatted, key-sorted JSON so Git diffs stay clean and minimal."""
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def collect_vehicle_positions(fetch_time: str) -> list[dict]:
    try:
        feed = fetch_protobuf(VEHICLE_POS_URL)
    except Exception as e:
        print(f"[Error] Failed to fetch Vehicle Positions: {e}")
        return []

    records = []
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            v = entity.vehicle
            
            timestamp = (
                datetime.fromtimestamp(v.timestamp, tz=timezone.utc).isoformat()
                if v.timestamp > 0
                else fetch_time
            )

            status_str = (
                gtfs_realtime_pb2.VehiclePosition.VehicleStopStatus.Name(v.current_status)
                if v.HasField("current_status")
                else None
            )

            records.append({
                "vehicle_id": v.vehicle.id if v.HasField("vehicle") else None,
                "vehicle_label": v.vehicle.label if v.HasField("vehicle") else None,
                "trip_id": v.trip.trip_id if v.HasField("trip") else None,
                "route_id": v.trip.route_id if v.HasField("trip") else None,
                "msg_timestamp": timestamp,
                "latitude": round(v.position.latitude, 6) if v.HasField("position") else None,
                "longitude": round(v.position.longitude, 6) if v.HasField("position") else None,
                "bearing": v.position.bearing if v.HasField("position") else None,
                "speed_mph": round(v.position.speed * 2.23694, 2) if v.HasField("position") and v.position.HasField("speed") else None,
                "current_stop_sequence": v.current_stop_sequence if v.HasField("current_stop_sequence") else None,
                "current_status": status_str,
            })

    # Sort deterministically by vehicle_id so array order never breaks Git diffs
    return sorted(records, key=lambda x: str(x.get("vehicle_id", "")))


def collect_trip_delays(fetch_time: str) -> list[dict]:
    try:
        feed = fetch_protobuf(TRIP_UPDATES_URL)
    except Exception as e:
        print(f"[Error] Failed to fetch Trip Updates: {e}")
        return []

    records = []
    for entity in feed.entity:
        if entity.HasField("trip_update"):
            tu = entity.trip_update
            trip_id = tu.trip.trip_id if tu.HasField("trip") else None
            route_id = tu.trip.route_id if tu.HasField("trip") else None
            vehicle_id = tu.vehicle.id if tu.HasField("vehicle") else None

            for stu in tu.stop_time_update:
                delay_sec = None
                if stu.HasField("arrival") and stu.arrival.HasField("delay"):
                    delay_sec = stu.arrival.delay
                elif stu.HasField("departure") and stu.departure.HasField("delay"):
                    delay_sec = stu.departure.delay

                if delay_sec is not None:
                    if delay_sec > 60:
                        status = "LATE"
                    elif delay_sec < -60:
                        status = "EARLY"
                    else:
                        status = "ON_TIME"

                    records.append({
                        "trip_id": trip_id,
                        "stop_sequence": stu.stop_sequence,
                        "stop_id": stu.stop_id,
                        "route_id": route_id,
                        "vehicle_id": vehicle_id,
                        "delay_seconds": delay_sec,
                        "delay_minutes": round(delay_sec / 60.0, 2),
                        "status": status,
                    })

    # Sort deterministically by trip_id and stop_sequence
    return sorted(records, key=lambda x: (str(x.get("trip_id", "")), x.get("stop_sequence", 0)))


def collect_alerts() -> list[dict]:
    try:
        feed = fetch_protobuf(ALERTS_URL)
    except Exception as e:
        print(f"[Error] Failed to fetch Alerts: {e}")
        return []

    records = []
    for entity in feed.entity:
        if entity.HasField("alert"):
            a = entity.alert
            header = a.header_text.translation[0].text if a.header_text.translation else None
            description = a.description_text.translation[0].text if a.description_text.translation else None
            cause = gtfs_realtime_pb2.Alert.Cause.Name(a.cause) if a.cause else None
            effect = gtfs_realtime_pb2.Alert.Effect.Name(a.effect) if a.effect else None

            if a.informed_entity:
                for ie in a.informed_entity:
                    records.append({
                        "alert_id": entity.id,
                        "route_id": ie.route_id if ie.HasField("route_id") else None,
                        "stop_id": ie.stop_id if ie.HasField("stop_id") else None,
                        "cause": cause,
                        "effect": effect,
                        "header": header,
                        "description": description,
                    })
            else:
                records.append({
                    "alert_id": entity.id,
                    "route_id": None,
                    "stop_id": None,
                    "cause": cause,
                    "effect": effect,
                    "header": header,
                    "description": description,
                })

    return sorted(records, key=lambda x: str(x.get("alert_id", "")))


if __name__ == "__main__":
    now_iso = datetime.now(timezone.utc).isoformat()
    print(f"Executing KAT GTFS-RT JSON Snapshot at {now_iso}...")

    vehicles = collect_vehicle_positions(now_iso)
    delays = collect_trip_delays(now_iso)
    alerts = collect_alerts()

    if vehicles:
        save_json_snapshot(vehicles, "data/vehicle_positions.json")
    if delays:
        save_json_snapshot(delays, "data/trip_updates.json")
    if alerts:
        save_json_snapshot(alerts, "data/alerts.json")

    print(f"Successfully recorded snapshot ({len(vehicles)} vehicles, {len(delays)} delays, {len(alerts)} alerts).")
