const express = require("express");
require("dotenv").config();

const mqttService = require("./services/mqttService");
const deviceRoutes = require("./routes/deviceRoutes");

const app = express();
const PORT = 3000;

app.use(express.json());

mqttService.startMQTT();

app.use("/device", deviceRoutes);

app.listen(PORT, () => {
    console.log("================================");
    console.log("Server Running");
    console.log(`http://localhost:${PORT}`);
    console.log("================================");
});