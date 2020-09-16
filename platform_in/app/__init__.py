from flask import Flask
import os
from elasticapm.contrib.flask import ElasticAPM
import logging
from flask import jsonify, request
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import certifi

logging.basicConfig(level=logging.INFO)
elastic_apm = ElasticAPM()

success_response_object = {"status":"success"}
success_code = 202
failure_response_object = {"status":"failure"}
failure_code = 400

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def kafka_avro_produce(avroProducer, topic, data):

    try:
        avroProducer.produce(topic=topic, value=data)
        logging.debug("avro produce")
        avroProducer.poll(2)
        if len(avroProducer) != 0:
            return False
    except BufferError:
        logging.error("local buffer full", len(avroProducer))
        return False
    except Exception as e:
        logging.error(e)
        return False

    return True

def create_app(script_info=None):

    # instantiate the app
    app = Flask(__name__)

    # set config
    app_settings = os.getenv("APP_SETTINGS")
    app.config.from_object(app_settings)

    # set up extensions
    elastic_apm.init_app(app)

    value_schema = avro.load("avro/movingvehicle.avsc")
    avroProducer = AvroProducer(
        {
            "bootstrap.servers": app.config["KAFKA_BROKERS"],
            "security.protocol": app.config["SECURITY_PROTOCOL"],
            "sasl.mechanism": app.config["SASL_MECHANISM"],
            "sasl.username": app.config["SASL_UNAME"],
            "sasl.password": app.config["SASL_PASSWORD"],
            "ssl.ca.location": certifi.where(),
            #"debug": "security,cgrp,fetch,topic,broker,protocol",
            "on_delivery": delivery_report,
            "schema.registry.url": app.config["SCHEMA_REGISTRY_URL"]
        },
        default_value_schema=value_schema,
    )

    # shell context for flask cli
    @app.shell_context_processor
    def ctx():
        return {"app": app}

    @app.route("/")
    def hello_world():
        return jsonify(hello="world")

    @app.route('/wrm247/v1/', methods=['POST'])
    def post_movingvehicle_data():
        try:
            print(request.data)
            data = request.get_json()

            print(request.headers)
            logging.debug(request.data)
            topic = "finnest.cesva.movingvehicle"

            kafka_avro_produce(avroProducer,topic,data)

            return success_response_object,success_code

        except Exception as e:
            avroProducer.flush()
            elastic_apm.capture_exception()
            return failure_response_object,failure_code

    return app