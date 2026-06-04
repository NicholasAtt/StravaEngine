import requests
import sys
from datetime import datetime, timezone

class StravaIngestor:
    def __init__(self, token, logstash_url):
        self.headers = {"Authorization": f"Bearer {token}"}
        self.logstash_url = logstash_url

    def fetch_activities(self, limit=50):
        """Recupera la lista delle attività dell'atleta"""
        url = "https://www.strava.com/api/v3/athlete/activities"
        res = requests.get(url, headers=self.headers, params={"per_page": limit})
        if res.status_code != 200:
            print(f"Errore Strava API: {res.text}")
            sys.exit(1)
        return res.json()

    def fetch_streams(self, activity_id):
        """Recupera i punti GPS e biometrici dell'attività selezionata"""
        url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
        params = {"keys": "time,latlng,altitude,heartrate,cadence,distance", "key_by_type": "true"}
        res = requests.get(url, headers=self.headers, params=params)
        return res.json()

    def parse_streams(self, streams, start_time):
        """Trasforma i json streams in una lista pulita di punti (dizionari)"""
        if 'time' not in streams or 'latlng' not in streams:
            return []

        times = streams['time']['data']
        latlngs = streams['latlng']['data']
        altitudes = streams.get('altitude', {}).get('data', [None] * len(times))
        heartrates = streams.get('heartrate', {}).get('data', [None] * len(times))
        cadences = streams.get('cadence', {}).get('data', [None] * len(times))
        distances = streams.get('distance', {}).get('data', [None] * len(times))

        points = []
        for i in range(len(times)):
            elapsed_seconds = times[i]
            point_time = start_time.timestamp() + elapsed_seconds
            t = datetime.fromtimestamp(point_time, tz=timezone.utc)
            lat, lon = latlngs[i] if len(latlngs[i]) == 2 else (0.0, 0.0)

            point = {
                'stream_type': 'strava', 
                'lat': lat, 'lon': lon, 'ele': altitudes[i],
                'time': t.isoformat(), '_time_obj': t
            }
            
            if heartrates[i] is not None: point['hr'] = heartrates[i]
            if cadences[i] is not None: point['cad'] = cadences[i]
            if distances[i] is not None: point['dist'] = distances[i]
            
            points.append(point)
        
        return points

    def send_point(self, point, session_id, req_session, realtime):
        """Invia un singolo punto al Logstash (Topic Veloce)"""
        payload = {k: v for k, v in point.items() if not k.startswith('_')}
        payload['session_id'] = session_id
        
        if realtime:
            payload['time'] = datetime.now(timezone.utc).isoformat()
            
        payload['ingestion_timestamp'] = datetime.now(timezone.utc).isoformat()

        try:
            req_session.post(self.logstash_url, json=payload, timeout=5.0, verify=False)
            sys.stdout.write(f"\r[STRAVA] Inviato: Lat={payload['lat']:.4f} Lon={payload['lon']:.4f} HR={payload.get('hr', 'N/A')} ")
            sys.stdout.flush()
        except requests.exceptions.RequestException:
            pass