import json
from datetime import datetime, timedelta
import pytz
import sys
from pprint import pprint as pp

def isDST(fecha, time_zone="America/Monterrey"):
    timezone = pytz.timezone(time_zone)
    c = fecha.astimezone(timezone)
    return bool(c.dst())

def getTimeZoneOffset(fecha, time_zone):
    try:
        timezone = pytz.timezone(time_zone)
        c = fecha.astimezone(timezone)
        if isDST(fecha, time_zone):
            return (c.utcoffset().total_seconds() / 60 / 60) - 1
        return c.utcoffset().total_seconds() / 60 / 60

    except Exception as e:
        print("Error al calcular offset")
        info = sys.exc_info()
        print(e, info)
    return -6

def generar_xmls(conteo, data, fecha):
    # TODO: Verificar DST
    try:
        
        values = {
            "macAddress": data["macAddress"],
            "SiteName": data["SiteName"],
            "IPv4Address": data["IPv4Address"],
            "FriendlyDeviceSerial": data["FriendlyDeviceSerial"],
            "DeviceName": data["DeviceName"],
            "Timestamp": fecha.strftime("%Y-%m-%dT%H:00:00"),
            "entrada": conteo["entrada"],
            "salida": conteo["salida"],
            "TZOffset": getTimeZoneOffset(fecha, data["TimeZone"]),
            "DST": "0",
        }
        xml_string = """<?xml version="1.0"?>
<DataRecords ID="{macAddress}" Name="{SiteName}" NoSensors="1" TZOffset="{TZOffset}" DST="{DST}" Setup="2020-12-01T16:13:33" Ver="2.9d" IP="{IPv4Address}" Port="1000">
<Record><TimeStamp>{Timestamp}</TimeStamp><HIndex>1</HIndex><PointID>{FriendlyDeviceSerial}_1</PointID><SensorID>{FriendlyDeviceSerial}</SensorID><SensorName>{DeviceName}</SensorName><SensorType>18</SensorType><PointIndex>1</PointIndex><PointType>1</PointType><Value>Okay</Value><Units>na</Units><Status>0</Status></Record>
<Record><TimeStamp>{Timestamp}</TimeStamp><HIndex>1</HIndex><PointID>{FriendlyDeviceSerial}_2</PointID><SensorID>{FriendlyDeviceSerial}</SensorID><SensorName>{DeviceName}</SensorName><SensorType>18</SensorType><PointIndex>2</PointIndex><PointType>2</PointType><Value>{entrada}</Value><Units>count</Units><Scale>1.000</Scale><Offset>0</Offset><Status>0</Status></Record>
<Record><TimeStamp>{Timestamp}</TimeStamp><HIndex>1</HIndex><PointID>{FriendlyDeviceSerial}_3</PointID><SensorID>{FriendlyDeviceSerial}</SensorID><SensorName>{DeviceName}</SensorName><SensorType>18</SensorType><PointIndex>3</PointIndex><PointType>2</PointType><Value>{salida}</Value><Units>count</Units><Scale>1.000</Scale><Offset>0</Offset><Status>0</Status></Record>
</DataRecords>""".format(
            **values
        )
        return xml_string
    except Exception as e:
        info = sys.exc_info()
        print(info)
#         publicar.error(e, info)
        
def get_values_json(data):
    values = {
        "macAddress": None,
        "SiteName": None,
        "IPv4Address": None,
        "FriendlyDeviceSerial": None,
        "DeviceName": None,
        "TimeZone": None,
        "datos": {},
        "tipo": None,
    }
    if "CountLogs" in data:
        for countLog in data["CountLogs"]:
            if "ClockChangedFrom" in countLog:
                values["ClockChanged"] = str(countLog["ClockChangedFrom"]) + "*" + str(countLog["Timestamp"])

            if "Counts" in countLog:
                if "Timestamp" in countLog:
                    p_fecha = datetime.strptime(
                        countLog["Timestamp"], "%Y-%m-%dT%H:%M:%SZ"
                    )
                    if not p_fecha in values["datos"]:
                        values["datos"][p_fecha] = {
                            "registros": {},
                            "entrada": 0,
                            "salida": 0,
                        }
                else:
                    continue
                for count in countLog["Counts"]:

                    if (
                        count["Name"] == "Line 1"
                        or count["Name"] == "Entrada"
                        or count["Name"] == "Entradas"
                        or count["Name"] == "In 1"
                        or "entrada" in count["Name"].lower()
                    ):
                        values["datos"][p_fecha]["entrada"] = count["Value"]
                    if (
                        count["Name"] == "Line 2"
                        or count["Name"] == "Salida"
                        or count["Name"] == "Salidas"
                        or count["Name"] == "Out 1"
                        or "salida" in count["Name"].lower()
                    ):
                        values["datos"][p_fecha]["salida"] = count["Value"]
                    values["datos"][p_fecha]["registros"][count["Name"]] = count
                    if count["Name"] == "Occupancy":
                        values["datos"][p_fecha]["registros"]["ocupacion"] = count[
                            "Value"
                        ]
                        values["tipo"] = "ocupacion"
    if "macAddress" in data:
        values["macAddress"] = data["macAddress"]
    if "SiteName" in data:
        values["SiteName"] = data["SiteName"]
    if "IPv4Address" in data:
        values["IPv4Address"] = data["IPv4Address"]

    if "FriendlyDeviceSerial" in data:
        values["FriendlyDeviceSerial"] = data["FriendlyDeviceSerial"]
    if "DeviceName" in data:
        values["DeviceName"] = data["DeviceName"]
    if "TimeZone" in data:
        values["TimeZone"] = data["TimeZone"]
    if len(values["datos"]) > 0:
        return values
    

"""full_val = '{"CountLogs": [{"ClockChangedFrom": "2022-06-03T19:55:16Z", "Timestamp": "2022-06-03T20:15:28Z", "TimestampLocaltime": "2022-06-03T15:15:28-05:00"}, {"Counts": [{"LogPeriodValue": 1, "Name": "Line 1", "RegisterId": 0, "RegisterType": "Line", "Tags": [], "UUID": "2e58b68f-9a61-4288-8ab1-786041ddb822", "Value": 63}, {"LogPeriodValue": 2, "Name": "Line 2", "RegisterId": 1, "RegisterType": "Line", "Tags": [], "UUID": "b063f0f2-be8e-4ca4-8524-04a361d0eca1", "Value": 62}], "LogEntryId": 53121, "StartTimestamp": "2022-06-03T19:50:00Z", "StartTimestampLocaltime": "2022-06-03T14:50:00-05:00", "Timestamp": "2022-06-03T20:20:00Z", "TimestampLocaltime": "2022-06-03T15:20:00-05:00"}], "DeviceID": "D001", "DeviceName": "DefaultName", "EnableDST": true, "FriendlyDeviceSerial": "V4D-21060107", "HistogramLogs": [{"ClockChangedFrom": "2022-06-03T19:55:16Z", "Timestamp": "2022-06-03T20:15:28Z", "TimestampLocaltime": "2022-06-03T15:15:28-05:00"}, {"Histograms": [], "LogEntryId": 53120, "StartTimestamp": "2022-06-03T19:50:00Z", "StartTimestampLocaltime": "2022-06-03T14:50:00-05:00", "Timestamp": "2022-06-03T20:20:00Z", "TimestampLocaltime": "2022-06-03T15:20:00-05:00"}], "IPv4Address": "192.168.1.54", "IPv6Address": "::", "SiteID": "S001", "SiteName": "DefaultSiteName", "TimeZone": "America/Mexico_City", "UserString": "-", "macAddress": "00:21:AC:04:31:CB"}'
full_dict = json.loads(full_val)
#print('diccionario del original')
#pp(full_dict)
test = get_values_json(full_dict)

for fecha, conteo in test["datos"].items():
    tmp = generar_xmls(conteo, test, fecha)
    print(tmp)
"""