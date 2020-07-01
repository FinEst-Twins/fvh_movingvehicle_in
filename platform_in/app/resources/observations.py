from flask import jsonify, request, Blueprint, current_app
from flask_restful import Resource, Api, reqparse, abort
from datetime import datetime
import json
import os
from rq import Queue, Connection
import logging
import time
import logging
from confluent_kafka import Producer, avro
from confluent_kafka.avro import AvroProducer


observations_blueprint = Blueprint("observations", __name__)
api = Api(observations_blueprint)
success_response_object = {"status": "success"}
success_code = 202
failure_response_object = {"status": "failure"}
failure_code = 400


def get_kafka_producer():
    return Producer(
        {
            "bootstrap.servers": current_app.config["KAFKA_BROKERS"],
            "security.protocol": current_app.config["SECURITY_PROTOCOL"],
            "sasl.mechanism": current_app.config["SASL_MECHANISM"],
            "sasl.username": current_app.config["SASL_UNAME"],
            "sasl.password": current_app.config["SASL_PASSWORD"],
            "ssl.ca.location": current_app.config["CA_CERT"],
        }
    )


def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def kafka_produce_peoplecounter_data(topic, jsonstring):

    kafka_producer = get_kafka_producer()

    try:
        kafka_producer.produce(
            topic, jsonstring, callback=delivery_report
        )
        kafka_producer.poll(2)
    except BufferError:
        logging.error("local buffer full", len(kafka_producer))
        elastic_apm.capture_exception()
        return False
    except Exception as e:
        elastic_apm.capture_exception()
        logging.error(e)
        return False


class SolarInverterObservation(Resource):
    def post(self):
        """
        Post new observation

        """
        try:
            data = request.get_json()
            data = json.loads(data)
            #print(request.headers)
            logging.info(f"post observation: {data}")
            #print("post data for solar inverter", data)
            #print("inverter name", data["name"])
        
            inverter_name = data["name"]
            topic_prefix = "test.sputhan.finest.viikkisolar"

            topic = f"{topic_prefix}.{inverter_name}"
            #print(topic)
            kafka_produce_peoplecounter_data(topic, json.dumps(data))
            return success_response_object,success_code

        except Exception as e:
            print("solar inverter error", e)
            return failure_response_object,failure_code


api.add_resource(SolarInverterObservation, "/viikkisolar/observation")
