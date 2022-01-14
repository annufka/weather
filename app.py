from datetime import datetime
from pprint import pprint

from flask import Flask
from flask_restful import Resource, Api
import requests

from config import API_WEATHER, coordinats
from sqlite import Database

db = Database()
app = Flask(__name__)
api = Api(app)

db.create_table_cities()
db.create_table_weather()


# '/'
class Home(Resource):
    def get(self):
        return {'result': 'Hello, world'}


# '/get_weather'
class WeatherFromAPI(Resource):
    def get_weather(self, lat, lon):
        response = requests.get(
            f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude=current,minutely,hourly,alerts&units=metric&appid={API_WEATHER}&lang=ru")
        return response.json()

    def get(self):
        for item in coordinats:
            # добавляю город в таблицу. Можно было через селект сделать проверку на наличие такого города (у меня даже есть
            # метод), но я решила через трай/эксепт
            try:
                db.add_city(item.get("city"))
            except:
                pass

            weather_info = self.get_weather(item.get("lat"), item.get("lon"))

            for item_day in weather_info["daily"]:
                db.add_weather(item.get("city"), datetime.fromtimestamp(item_day.get("dt")),
                               round((item_day.get("temp").get("min") + item_day.get("temp").get("max")) / 2, 2),
                               item_day.get("pop"), item_day.get("clouds"), item_day.get("pressure"),
                               item_day.get("humidity"),
                               item_day.get("wind_speed"))

        return {"result": "OK"}


# '/cities'
class Cities(Resource):
    def get(self):
        result = [i[0] for i in db.select_all_cities()]
        return {"result": result}


# '/mean/clouds/Odesa'
class Value(Resource):
    def get(self, value_type, city):
        result = db.select_params(value_type, city)
        sum = 0
        for item in result:
            sum += item[0]
        return {"result": sum / len(result)}


# '/records/Odesa/2021-12-25/2021-12-28'
class Records(Resource):
    def get(self, city, start_dt, end_dt):
        result_from_db = db.select_params_by_city_date(start_dt, end_dt, city)
        result = []
        for item in result_from_db:
            one_day = {
                "dt": item[1],
                "temp": item[2],
                "pop": item[3],
                "clouds": item[4],
                "pressure": item[5],
                "humidity": item[6],
                "wind_speed": item[7]
            }
            result.append(one_day)
        return {'result': result}
    
# '/moving_mean/value_type/city'
class MovingAverage(Resource):
    def get(self, value_type, city):
        result = db.select_params(value_type, city)
        sum = 0
        for item in result:
            sum += item[0]
        return {"result": sum / len(result)}


api.add_resource(Home, '/')
api.add_resource(WeatherFromAPI, '/get_weather')
api.add_resource(Cities, '/cities')
api.add_resource(Value, '/mean/<value_type>/<city>')
api.add_resource(Records, '/records/<city>/<start_dt>/<end_dt>')
api.add_resource(MovingAverage, '/moving_mean/value_type/city')

if __name__ == '__main__':
    app.run()
