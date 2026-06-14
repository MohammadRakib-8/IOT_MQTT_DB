const express = require("express");
const mqtt = require("mqtt");

const app = express();
const PORT = 3000;

let latestData = {
  temperature: 0,
  humidity: 0,
  voltage: 0,
  current: 0,
  relayState: 0,
};

5



const client = mqtt.connect(MQTT_BROKER, {
  username: MQTT_USER,
  password: MQTT_PASS,
});

client.on("connect", () => {
  console.log(" MQTT Connected");

  client.subscribe(MQTT_TOPIC, () => {
    console.log(" Subscribed:", MQTT_TOPIC);
  });
});

client.on("message", (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    let relayState = 0;

    if (data.temperature > 30) {
      relayState = 1;
    }

    latestData = {
      ...data,
      relayState,
    };

    console.log(" MQTT Data:", latestData);

  } catch (error) {
    console.log(" Invalid JSON");
  }
});

app.get("/device/latest", (req, res) => {
  res.json(latestData);
});

app.listen(PORT, () => {
  console.log(` Server Running: http://localhost:${PORT}`);
});