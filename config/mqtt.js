const mqtt = require("mqtt");
require("dotenv").config();

const client = mqtt.connect(process.env.MQTT_BROKER, {
    username: process.env.MQTT_USER,
    password: process.env.MQTT_PASS
});

module.exports = client;