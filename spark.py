import statistics
import sys

sys.path.append('/usr/lib/python3/dist-packages')
import requests
import json
from datetime import date
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Proyecto final Almacenamiento').getOrCreate()
sc = spark.sparkContext

def main():
    run = True
    while run:
        print("Obteniendo datos de sensores")
        url = "http://104.248.128.237:5050/nodemcu?page=1"
        response = requests.get(url)
        data = response.text
        parsed = json.loads(data)
        rdd = sc.parallelize(parsed)
        rdd = spark.read.json(rdd)
        rdd.show()
        rdd.printSchema()
        rdd.createOrReplaceTempView("registro")
        temp = spark.sql("SELECT AVG(temperatura) AS promedio_sensor_temp FROM registro WHERE fecha BETWEEN '2021-05-24' AND '2021-05-27'")
        humedad = spark.sql("SELECT AVG(humedad) AS promedio_sensor_hum FROM registro WHERE fecha BETWEEN '2021-05-24' AND '2021-05-27'")
        sonico = spark.sql("SELECT AVG(sonico) AS promedio_sensor_sonic FROM registro WHERE fecha BETWEEN '2021-05-24' AND '2021-05-27'")
        temp.show()
        humedad.show()
        sonico.show()
        R = input("programa en pausa")
        if R != 1:
            run = False


if __name__ == '__main__':
    main()
