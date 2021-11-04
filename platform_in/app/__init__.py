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
    sentry_sdk.init(dsn=os.getenv("SENTRY_DSN"), integrations=[FlaskIntegration()])



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

    logging.basicConfig(level=app.config["LOG_LEVEL"])
    logging.getLogger().setLevel(app.config["LOG_LEVEL"])

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
            # data = request.get_data()
            data = request.get_data()
            logging.info(f"post data goes like : {data[0:200]}")
            json_data = json.loads(data)
            logging.debug(f"post data in json : {json_data}")

            # ts and v are separate array.
            # converting it into an array of ts,v pairs here

            ts_arr = json_data["ts"]
            v_arr = json_data["v"]

            pair_arr = []

            if len(ts_arr) != len(v_arr):
                raise Exception("error in input, array length ts and v do not match")


            iter = 0
            for item in ts_arr:
                pair_arr.append({"ts":item ,"v":v_arr[iter]})
                iter = iter + 1

            print(pair_arr)

            json_data["values"] = pair_arr
            del json_data["ts"]
            del json_data["v"]

            producer.send(
                topic="finest.json.movingvehicle",
                # topic="test.sputhan",
                key="",
                value=request.get_json(),
            )

            return success_response_object, success_code

        except Exception as e:
            producer.flush()
            logging.error("post data error", e)
            # elastic_apm.capture_exception()
            return failure_response_object, failure_code

    return app
