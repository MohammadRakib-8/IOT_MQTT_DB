const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../../.env") }); 

const client = require("../config/mqtt");
const db = require("../config/db"); 
const Redis = require("ioredis");
const mysql = require("mysql2/promise");

const redis = new Redis();

// MySQL Connection

const mysqlPool = mysql.createPool({
  host: process.env.MYSQL_HOST,
  port: parseInt(process.env.MYSQL_PORT || "3306"),
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DB,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Data Handlers (Unchanged)

async function closePreviousPeriod(connection, deviceId, sensorType, sensorNum, deviceTime) {
  const cleanDataQuery = `INSERT INTO dems_sensor_clean_logs(device_id, sensor_type, sensor_num, sensor_value, alert, device_time) VALUES($1, $2, $3, $4, $5, $6)`;
  const redisKey = `device:last:${deviceId}:${sensorType}`;
  const [prevValueStr, prevTimeStr] = await redis.hmget(redisKey, "value", "time");
  if (prevValueStr !== null && prevTimeStr !== null) {
    await connection.query(cleanDataQuery, [deviceId, sensorType, sensorNum, prevValueStr, 0, prevTimeStr]);
  }
}

async function handleDHT22Model(connection, data, deviceId, deviceTime) {
  const cleanDataQuery = `INSERT INTO dems_sensor_clean_logs(device_id, sensor_type, sensor_num, sensor_value, alert, device_time) VALUES($1, $2, $3, $4, $5, $6)`;
  const changedSensors = {};

  if (data.T !== undefined && data.T !== null) {
    const redisKey = `device:last:${deviceId}:t`;
    if (data.ft === 1) {
      await closePreviousPeriod(connection, deviceId, "t", data.Sid, deviceTime);
      await connection.query(cleanDataQuery, [deviceId, "t", data.Sid, data.T, 0, deviceTime]);
      await redis.hmset(redisKey, { value: data.T.toString(), time: deviceTime });
      changedSensors.temp = data.T;
    } else {
      await redis.hset(redisKey, "time", deviceTime);
    }
  }

  if (data.H !== undefined && data.H !== null) {
    const redisKey = `device:last:${deviceId}:h`;
    if (data.fh === 1) {
      await closePreviousPeriod(connection, deviceId, "h", data.Sid, deviceTime);
      await connection.query(cleanDataQuery, [deviceId, "h", data.Sid, data.H, 0, deviceTime]);
      await redis.hmset(redisKey, { value: data.H.toString(), time: deviceTime }); 
      changedSensors.hum = data.H;
    } else {
      await redis.hset(redisKey, "time", deviceTime);
    }
  }
  return changedSensors;
}

async function handleMQ2Model(connection, data, deviceId, deviceTime) {
  const cleanDataQuery = `INSERT INTO dems_sensor_clean_logs(device_id, sensor_type, sensor_num, sensor_value, alert, device_time) VALUES($1, $2, $3, $4, $5, $6)`;
  const changedSensors = {};
  if (data.gs !== undefined) {
    const redisKey = `device:last:${deviceId}:mq2`;
    if (data.gf === 1) {
      await closePreviousPeriod(connection, deviceId, "mq2", data.Sid, deviceTime);
      await connection.query(cleanDataQuery, [deviceId, "mq2", data.Sid, data.gs, 0, deviceTime]);
      await redis.hmset(redisKey, { value: data.gs.toString(), time: deviceTime });
      changedSensors.mq2 = data.gs;
    } else {
      await redis.hset(redisKey, "time", deviceTime);
    }
  }
  return changedSensors;
}

function getHandlerForModel(data) {
  switch (data.mdl) {
    case "dht22": return handleDHT22Model;
    case "mq2": return handleMQ2Model;
    default: return null;
  }
}

async function insertRawAndHeartbeat(connection, data, deviceId, deviceTime) {
  await connection.query(`INSERT INTO dems_raw_data_logs(device_id, raw_data, device_time) VALUES ($1, $2, $3)`, [deviceId, JSON.stringify(data), deviceTime]);
  if (data.Connectivity === "online" || data.Connectivity === "offline") {
    await connection.query(`INSERT INTO dems_device_heartbeat(device_mac, connectivity) VALUES ($1, $2)`, [deviceId, data.Connectivity]);
  }
}

// Threshold Evaluation (With Redis Publish & 15-Minute Cooldown)

async function evaluateThresholds(pgConnection, deviceIdMac, sensorType, sensorNum, sensorValue, deviceTime) {
  if (sensorValue === null || sensorValue === undefined) return;

  const configsQuery = `
    SELECT sc.id as sensor_config_id, sc.user_id, d.id as device_id, sc.min_threshold, sc.max_threshold, sc.is_active
    FROM sensor_configs sc
    JOIN devices d ON sc.device_id = d.id
    WHERE d.device_mac = ? 
      AND sc.sensor_type = ? 
      AND sc.sensor_num = ? 
      AND sc.is_active = 1
      AND CURTIME() BETWEEN sc.schedule_start AND sc.schedule_end
  `;
  
  try {
    const [configsRows] = await mysqlPool.query(configsQuery, [deviceIdMac, sensorType, String(sensorNum)]);
    if (configsRows.length === 0) return;

    for (const config of configsRows) {
      // Safety check
      if (!config.is_active || Number(config.is_active) !== 1) continue;

      if (sensorValue < config.min_threshold || sensorValue > config.max_threshold) {
        
        // 15 MINUTE COOLDOWN LOGIC USING REDIS
        // Include thresholds in the cooldown key to bypass old cooldowns immediately when the config is changed
        const cooldownKey = `alert:cooldown:${config.device_id}:${config.sensor_config_id}:${config.min_threshold}:${config.max_threshold}`;
        const isOnCooldown = await redis.exists(cooldownKey);
        
        if (isOnCooldown) {
          // Alert was triggered recently for this specific sensor config, skip DB insert
          continue;
        }

        const insertAlertQuery = `
          INSERT INTO device_alerts (
            user_id, device_id, sensor_config_id, sensor_type, sensor_num, 
            sensor_value, alert, is_notification_sent, alert_type, triggered_at, created_at, updated_at,
            min_threshold, max_threshold
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW(), $11, $12)
          RETURNING id
        `;
        
        const inserted = await pgConnection.query(insertAlertQuery, [
          config.user_id, 
          config.device_id, 
          config.sensor_config_id,
          sensorType, 
          String(sensorNum), 
          sensorValue,
          true,         // alert
          false,        // is_notification_sent
          'Not set',    // alert_type
          deviceTime,   // triggered_at
          config.min_threshold,
          config.max_threshold
        ]);
        
        const alertId = inserted.rows && inserted.rows[0] ? inserted.rows[0].id : null;

        if (alertId) {
          // SET COOLDOWN IN REDIS FOR 15 MINUTES (900 seconds)
          await redis.set(cooldownKey, alertId, 'EX', 901);
          
          console.log(`🚨 [ALERT SAVED] ID: ${alertId} | Device: ${deviceIdMac} | Sensor: ${sensorType}#${sensorNum} | Cooldown activated for 15 mins`);
          
          // PUBLISH TO REDIS FOR LARAVEL TO LISTEN
          await redis.publish("dems_alert_channel", JSON.stringify({
            alert_id: alertId, 
            user_id: config.user_id, 
            device_id: config.device_id,
            sensor_type: sensorType,    
            sensor_num: sensorNum       
          }));
          console.log(`📡 [REDIS PUBLISHED] Alert ID ${alertId} sent to Laravel channel.`);
        }
      }
    }
  } catch (dbErr) {
    console.error(`[Threshold DB Error]`, dbErr.message);
  }
}

// MQTT Listener

function startMQTT() {
  console.log("startmqtt called");

  client.on("connect", () => {
    console.log("MQTT Connected");
    client.subscribe(process.env.MQTT_TOPIC);
    client.subscribe(process.env.MQTT_TOPIC_DEVICE_CONNECTIVITY);
  });

  client.on("message", async (topic, message) => {
    let rawMessage = message.toString();
    let data;
    
    try {
      if (rawMessage.includes('NaN')) rawMessage = rawMessage.replace(/NaN/g, 'null');
      data = JSON.parse(rawMessage);
      console.log(data);
    } catch (e) {
      return;
    }

    const deviceId = data.mac;
    if (!deviceId) return;

    let deviceTime = data.time ? new Date((data.time * 1000) - (6 * 60 * 60 * 1000)).toISOString() : new Date().toISOString();
    const handler = getHandlerForModel(data);

    const changedSensors = {};
    if (handler) {
      if (data.T !== undefined && data.T !== null) changedSensors.temp = data.T;
      if (data.DemsTemp !== undefined) changedSensors.temp = data.DemsTemp;
      if (data.H !== undefined && data.H !== null) changedSensors.hum = data.H;
      if (data.DemsHum !== undefined) changedSensors.hum = data.DemsHum;
      if (data.gs !== undefined) changedSensors.mq2 = data.gs;
      if (data.mq2 !== undefined) changedSensors.mq2 = data.mq2;
    }

    if (Object.keys(changedSensors).length > 0 || data.Connectivity === "online" || data.Connectivity === "offline") {
      try {
        await redis.publish("device_updates", JSON.stringify({
          device_id: deviceId, model: data.mdl || "dems", time: deviceTime, ...changedSensors, connectivity: data.Connectivity || null
        }));
      } catch (pubErr) {}
    }

    const conn = await db.connect();
    try {
      await conn.query("BEGIN");
      await insertRawAndHeartbeat(conn, data, deviceId, deviceTime);
      if (handler) await handler(conn, data, deviceId, deviceTime);
      
      if (data.T !== undefined && data.T !== null) await evaluateThresholds(conn, deviceId, "t", data.Sid, data.T, deviceTime);
      if (data.H !== undefined && data.H !== null) await evaluateThresholds(conn, deviceId, "h", data.Sid, data.H, deviceTime);
      if (data.gs !== undefined) await evaluateThresholds(conn, deviceId, "mq2", data.Sid, data.gs, deviceTime);
      if (data.DemsTemp !== undefined) await evaluateThresholds(conn, deviceId, "t", data.Sid, data.DemsTemp, deviceTime);
      if (data.DemsHum !== undefined) await evaluateThresholds(conn, deviceId, "h", data.Sid, data.DemsHum, deviceTime);
      if (data.mq2 !== undefined) await evaluateThresholds(conn, deviceId, "mq2", data.Sid, data.mq2, deviceTime);

      await conn.query("COMMIT");
    } catch (err) {
      await conn.query("ROLLBACK");
      console.error("DB Error:", err.message);
    } finally {
      conn.release();
    }
  });
}

module.exports = { startMQTT };
