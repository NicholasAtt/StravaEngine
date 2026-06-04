import requests
from datetime import datetime, timezone

class EnvironmentIngestor:
    def __init__(self, logstash_url):
        self.logstash_url = logstash_url
        self.w_data = None
        self.a_data = None
        self.last_emitted_hour = None

    def fetch_hourly_data(self, lat, lon, dt_timestamp):
        """Scarica e salva in memoria i dati ambientali per l'intera giornata"""
        dt = datetime.fromtimestamp(dt_timestamp, tz=timezone.utc)
        date_str = dt.strftime('%Y-%m-%d')
        print(f"\n Scarico dati meteo e aria per il {date_str}...")
        
        try:
            w_url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={date_str}&end_date={date_str}&hourly=temperature_2m,relative_humidity_2m"
            self.w_data = requests.get(w_url, timeout=5.0).json()
            
            a_url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}&start_date={date_str}&end_date={date_str}&hourly=european_aqi,pm2_5"
            self.a_data = requests.get(a_url, timeout=5.0).json()
        except Exception as e:
            print(f" Attenzione: impossibile scaricare dati meteo reali, userò valori default ({e})")

    def send_if_new_hour(self, dt_obj, session_id, req_session, realtime):
        """Invia i dati al Logstash SOLO se l'ora della simulazione è scattata (Topic Lento)"""
        current_hour = dt_obj.hour
        
        if current_hour != self.last_emitted_hour:
            print(f"\n Aggiornamento orario scattato: {current_hour}:00. Invio a Logstash...")
            
            temp, hum, aqi, pm25 = 20.0, 50.0, 50.0, 10.0

            if self.w_data and "hourly" in self.w_data:
                temp = float(self.w_data["hourly"]["temperature_2m"][current_hour])
                hum = float(self.w_data["hourly"]["relative_humidity_2m"][current_hour])
                
            if self.a_data and "hourly" in self.a_data:
                aqi = float(self.a_data["hourly"]["european_aqi"][current_hour])
                pm25 = float(self.a_data["hourly"]["pm2_5"][current_hour])

            time_str = datetime.now(timezone.utc).isoformat() if realtime else dt_obj.isoformat()

            req_session.post(self.logstash_url, json={
                "stream_type": "weather", "session_id": session_id, "time": time_str, 
                "temperature": temp, "humidity": hum
            }, verify=False)
            
            req_session.post(self.logstash_url, json={
                "stream_type": "airquality", "session_id": session_id, "time": time_str, 
                "aqi": aqi, "pm2_5": pm25
            }, verify=False)
            
            self.last_emitted_hour = current_hour