const express = require("express");
const router = express.Router();

const mqttService = require("../services/mqttService");

router.get("/latest", (req, res) => {
    res.json(mqttService.getLatestData());
});

module.exports = router;




