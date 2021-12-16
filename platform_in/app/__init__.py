from flask import Flask
import os
import sys
from flask import jsonify
from elasticapm.contrib.flask import ElasticAPM
import logging
from flask import jsonify, request
import json
import certifi
from kafka import KafkaProducer
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration

if os.getenv("SENTRY_DSN"):
    sentry_sdk.init(dsn=os.getenv("SENTRY_DSN"),
                    integrations=[FlaskIntegration()])


success_response_object = {"status": "success"}
success_code = 202
failure_response_object = {"status": "failure"}
failure_code = 400

elastic_apm = ElasticAPM()


def create_app(script_info=None):

    # instantiate the app
    app = Flask(__name__)

    # set config
    app_settings = os.getenv("APP_SETTINGS")
    app.config.from_object(app_settings)

    logging.basicConfig(format="[%(asctime)s] %(levelname)s in %(module)s: %(message)s", level=app.config["LOG_LEVEL"])
    logging.getLogger().setLevel(app.config["LOG_LEVEL"])
    logging.info("In create_app()")

    # set up extensions
    elastic_apm.init_app(app)
    producer = KafkaProducer(
        bootstrap_servers=app.config["KAFKA_BROKERS"],
        security_protocol=app.config["SECURITY_PROTOCOL"],
        ssl_cafile=app.config["CA_FILE"],
        ssl_certfile=app.config["CERT_FILE"],
        ssl_keyfile=app.config["KEY_FILE"],
        value_serializer=lambda v: json.dumps(v).encode("ascii"),
        key_serializer=lambda v: json.dumps(v).encode("ascii"),
    )

    # shell context for flask cli
    @app.shell_context_processor
    def ctx():
        return {"app": app}

    @app.route("/")
    def hello_world():
        return jsonify(health="ok")

    @app.route("/debug-sentry")
    def trigger_error():
        division_by_zero = 1 / 0

    @app.route("/wrm247/v1", methods=["POST"])
    def post_data():

        try:
            data = request.get_data()
            logging.info(f"post data goes like : {data[0:200]}")
            json_data = json.loads(data)
            logging.debug(f"post data in json : {json_data}")

            # Combine timeseries and values in payload
            payloads = json_data["payload"]
            formatted_payloads = []
            for payload in payloads:
                ts_arr, v_arr = payload["ts"], payload["v"] # should be of same length
                payload["values"] = list(map(lambda x,y: {"ts":x, "v":y}, ts_arr, v_arr))
                del payload["ts"], payload["v"]
                formatted_payloads.append(payload)

            json_data["payload"] = formatted_payloads
            
            producer.send(
                topic="finest.json.movingvehicle",
                key="", # might have to add dummy data here to remove null error in connector
                value=json_data,
            )

            return success_response_object, success_code

        except Exception as e:
            producer.flush()
            logging.error("post data error", e)
            # elastic_apm.capture_exception()
            return failure_response_object, failure_code

    return app
