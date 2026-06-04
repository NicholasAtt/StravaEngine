import argparse
import requests
import time
import sys
import uuid
import redis
import os
import urllib3
from datetime import datetime, timezone

from strava_ingestor import StravaIngestor
from env_ingestor import EnvironmentIngestor

REDIS_CLEANUP_DELAY_SECONDS = 10


class DataPipelineManager:
    """Orchestratore principale della simulazione"""
    
    def __init__(self, token, logstash_url, realtime):
        self.strava = StravaIngestor(token, logstash_url)
        self.env = EnvironmentIngestor(logstash_url)
        
        self.realtime = realtime
        self.req_session = requests.Session()
        self.redis_client = self._init_redis()

    def _init_redis(self):
        try:
            redis_host = os.getenv("REDIS_HOST", "redis")
            redis_port = int(os.getenv("REDIS_PORT", 6379))
            r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            print(" Pulizia chiavi stream in Redis...")
            r.delete("race:live_tracking", "race:live_metrics", "race:cardiac_drift_stream", "live_run:state")
            return r
        except Exception as e:
            print(f"Redis non disponibile ({e}).")
            return None

    def _generate_session_id(self, start_lat, start_lon):
        session_id = str(uuid.uuid4())
        if self.redis_client:
            try:
                cities = self.redis_client.georadius("sicily_cities", start_lon, start_lat, 100, "km", sort="ASC", count=1)
                city_name = cities[0] if cities else "sconosciuta_strava"
                prog = self.redis_client.incr(f"city_counter:{city_name}")
                session_id = f"corsa_{city_name}_{prog}"
            except Exception:
                pass
        return session_id

    def run(self):
        print("STAVA PIPELINE INGESTOR")
        
        activities = self.strava.fetch_activities()
        if not activities:
            print("Nessuna attività trovata.")
            return

        for i, act in enumerate(activities):
            print(f"[{i+1}] {act['name']} - {act['distance']/1000:.2f} km - Data: {act['start_date']}")
        
        try:
            choice = int(input("\n Seleziona l'attività da iniettare: "))
            selected = activities[choice - 1]
        except (ValueError, IndexError):
            print("Selezione non valida.")
            return
        
        start_time = datetime.strptime(selected['start_date'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        
        print("\nRecupero streams GPS/Biometrici...")
        streams = self.strava.fetch_streams(selected['id'])
        points = self.strava.parse_streams(streams, start_time)

        if not points:
            print("L'attività non contiene dati sufficienti.")
            return

        first_point = points[0]
        session_id = self._generate_session_id(first_point['lat'], first_point['lon'])
        print(f"Avviata sessione: {session_id}")

        if self.redis_client:
            self.redis_client.set("active_session_id", session_id)

        self.env.fetch_hourly_data(first_point['lat'], first_point['lon'], first_point['_time_obj'].timestamp())

        start_wall = time.time()
        first_ts = first_point['_time_obj'].timestamp()
        
        print("\n Avvio simulazione streaming in corso...")
        try:
            for point in points:
                current_ts = point['_time_obj'].timestamp()
                delay = (start_wall + (current_ts - first_ts)) - time.time()
                
                if delay > 0:
                    time.sleep(delay)

                self.env.send_if_new_hour(point['_time_obj'], session_id, self.req_session, self.realtime)

                self.strava.send_point(point, session_id, self.req_session, self.realtime)

        except KeyboardInterrupt:
            print("\n Interrotto dall'utente.")
        finally:
            if self.redis_client:
                print("\n Rimozione sessione attiva e pulizia stream Redis...")
                self.redis_client.delete("active_session_id")
                time.sleep(REDIS_CLEANUP_DELAY_SECONDS)
                self.redis_client.delete("race:live_tracking", "race:live_metrics", "race:cardiac_drift_stream", "live_run:state")
            self.req_session.close()
            print("\n Pipeline terminata.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Strava a oggetti")
    parser.add_argument('--token', default=os.getenv("STRAVA_TOKEN"), help="Access token API Strava (default: env STRAVA_TOKEN)")
    parser.add_argument('--url', default=os.getenv("LOGSTASH_URL", "http://logstash:8080/"), help="Endpoint URL di Logstash (default: env LOGSTASH_URL)")
    parser.add_argument('--realtime', action='store_true', help="Sostituisce i timestamp con l'orario corrente")
    args = parser.parse_args()

    if not args.token:
        parser.error("Token Strava obbligatorio: passalo con --token o imposta STRAVA_TOKEN nell'ambiente.")

    manager = DataPipelineManager(args.token, args.url, args.realtime)
    manager.run()
