'OpenWeatherMap to MQTT bridge'

import click
import json
import logging
import paho.mqtt.client
import requests
import schedule

__version__ = '1.0.0'

logging.basicConfig(format='%(asctime)s <%(levelname)s> %(message)s',
                    level=logging.DEBUG, datefmt='%Y-%m-%dT%H:%M:%S')


def job_fetch_weather(mqtt, key, city, topic):
    logging.debug('Starting scheduled job')
    try:
        r = requests.get('https://api.openweathermap.org/data/2.5/weather?id=%s&units=metric&APPID=%s' % (city, key))
        data = r.json()
    except KeyboardInterrupt:
        raise
    except:
        logging.error('Failed to fetch data from OpenWeatherMap')
    try:
        mqtt.publish(topic, qos=1, payload=data)
    except KeyboardInterrupt:
        raise
    except:
        logging.error('Failed to publish the data')


def on_connect(client, userdata, flags, rc):
    logging.info('MQTT connected (code: %d)' % rc)


@click.command()
@click.option('--host', '-h', default='127.0.0.1', help='MQTT broker host.')
@click.option('--port', '-p', default='1883', help='MQTT broker port.')
@click.option('--key', '-k', required=True, help='OpenWeatherMap API key.')
@click.option('--city', '-c', required=True, help='OpenWeatherMap city id.')
@click.option('--topic', '-t', required=True, help='MQTT topic to used for publishing.')
@click.option('--interval', '-i', default=15, type=click.IntRange(1, 99999), help='Interval in minutes between API calls.')
@click.version_option(version=__version__)
def main(host, port, key, city, topic, interval):
    try:
        logging.info('Program started')
        logging.getLogger('schedule').propagate = False
        logging.getLogger('urllib3').propagate = False
        mqtt = paho.mqtt.client.Client()
        mqtt.on_connect = on_connect
        mqtt.connect(host, int(port))
        schedule.every(interval).minutes.do(job_fetch_weather, mqtt, key, city, topic)
        while True:
            schedule.run_pending()
            mqtt.loop()
    except KeyboardInterrupt:
        pass
