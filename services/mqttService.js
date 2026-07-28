const client = require("../config/mqtt");
const db = require("../config/db");
const Redis = require("ioredis");

const redis = new Redis();

async function closePreviousPeriod(connection, deviceId, sensorType, sensorNum, deviceTime) {
  const cleanDataQuery = `
    INSERT INTO dems_sensor_clean_logs(device_id, sensor_type, sensor_num, sensor_value, alert, device_time)
    VALUES($1, $2, $3, $4, $5, $6)`;
    
  const redisKey = `device:last:${deviceId}:${sensorType}`;
  
  const [prevValueStr, prevTimeStr] = await redis.hmget(redisKey, "value", "time");
  
  if (prevValueStr !== null && prevTimeStr !== null) {
    await connection.query(cleanDataQuery, [deviceId, sensorType, sensorNum, prevValueStr, 0, prevTimeStr]);
    console.log(`[${sensorType}] ${deviceId} Closed previous period → Value: ${prevValueStr} at ${prevTimeStr}`);
  }
}

async function handleDemsModel(connection, data, deviceId, deviceTime) {
  const cleanDataQuery = `INSERT INTO 
    dems_sensor_clean_logs(device_id, sensor_type, sensor_num, sensor_value, alert, device_time)
    VALUES($1, $2, $3, $4, $5, $6)`;

  const changedSensors = {};


  if (data.mq2 !== undefined) {
    await connection.query(cleanDataQuery, [deviceId, "mq2", data.Sid, data.mq2, 0, deviceTime]);
    changedSensors.mq2 = data.mq2;
  }

  if (data.DemsHum !== undefined) {
    await connection.query(cleanDataQuery, [deviceId, "h", data.Sid, data.DemsHum, 0, deviceTime]);
    changedSensors.hum = data.DemsHum;
  }

  if (data.DemsTemp !== undefined) {
    await connection.query(cleanDataQuery, [deviceId, "t", data.Sid, data.DemsTemp, 0, deviceTime]);
    changedSensors.temp = data.DemsTemp;
  }

  return changedSensors;
}

async function handleDHT22Model(connection, data, deviceId, deviceTime) {
  const cleanDataQuery = `INSERT INTO 
    dems_sensor_clean_logs(device_id, sensor_type, sensor_num, sensor_value, alert, device_time)
    VALUES($1, $2, $3, $4, $5, $6)`;

  const changedSensors = {};

  if (data.T !== undefined) {
    const redisKey = `device:last:${deviceId}:t`;

    if (data.ft === 1) {
      await closePreviousPeriod(connection, deviceId, "t", data.Sid, deviceTime);
      await connection.query(cleanDataQuery, [deviceId, "t", data.Sid, data.T, 0, deviceTime]);
      console.log(`[DHT22] ${deviceId} Temp CHANGED → ${data.T}°C (inserted clean)`);
      
      await redis.hmset(redisKey, { value: data.T.toString(), time: deviceTime });
      changedSensors.temp = data.T;
    } else {
      await redis.hset(redisKey, "time", deviceTime);
      console.log(`[DHT22] ${deviceId} Temp unchanged → ${data.T}°C (skipped)`);
    }
  }

  if (data.H !== undefined) {
    const redisKey = `device:last:${deviceId}:h`;

    if (data.fh === 1) {
      await closePreviousPeriod(connection, deviceId, "h", data.Sid, deviceTime);
      await connection.query(cleanDataQuery, [deviceId, "h", data.Sid, data.H, 0, deviceTime]);
      console.log(`[DHT22] ${deviceId} Hum CHANGED → ${data.H}% (inserted clean)`);
      
      await redis.hmset(redisKey, { value: data.H.toString(), time: deviceTime });  //value: String(data.H) crash proof
      changedSensors.hum = data.H;
    } else {
      await redis.hset(redisKey, "time", deviceTime);
      console.log(`[DHT22] ${deviceId} Hum unchanged → ${data.H}% (skipped)`);
    }
  }

  return changedSensors;
}

async function handleMQ2Model(connection, data, deviceId, deviceTime) {
  const cleanDataQuery = `INSERT INTO 
    dems_sensor_clean_logs(device_id, sensor_type, sensor_num, sensor_value, alert, device_time)
    VALUES($1, $2, $3, $4, $5, $6)`;

  const changedSensors = {};

  if (data.gs !== undefined) {
    const redisKey = `device:last:${deviceId}:mq2`;

    if (data.gf === 1) {
      await closePreviousPeriod(connection, deviceId, "mq2", data.Sid, deviceTime);
      await connection.query(cleanDataQuery, [deviceId, "mq2", data.Sid, data.gs, 0, deviceTime]);
      console.log(`[MQ2] ${deviceId} Gas CHANGED → ${data.gs} (inserted clean)`);
      
      await redis.hmset(redisKey, { value: data.gs.toString(), time: deviceTime });
      changedSensors.mq2 = data.gs;
    } else {
      await redis.hset(redisKey, "time", deviceTime);
      console.log(`[MQ2] ${deviceId} Gas unchanged → ${data.gs} (skipped)`);
    }
  }

  return changedSensors;
}

function getHandlerForModel(data) {
  const model = data.mdl; 
  switch (model) {
    case "dht22": return handleDHT22Model;
    case "mq2": return handleMQ2Model;
    default:
      // if (data.DemsTemp !== undefined || data.DemsHum !== undefined || data.mq2 !== undefined) {
      //   return handleDemsModel;
      // }
      return null;
  }
}

async function insertRawAndHeartbeat(connection, data, deviceId, deviceTime) {
  await connection.query(
    `INSERT INTO dems_raw_data_logs(device_id, raw_data, device_time) VALUES ($1, $2, $3)`,
    [deviceId, JSON.stringify(data), deviceTime]
  );

  if (data.Connectivity === "online" || data.Connectivity === "offline") {
    await connection.query(
      `INSERT INTO dems_device_heartbeat(device_mac, connectivity) VALUES ($1, $2)`,
      [deviceId, data.Connectivity]
    );
  }
}

function startMQTT() {
  console.log("startmqtt called");

  client.on("connect", () => {
    console.log("MQTT Connected");
    client.subscribe(process.env.MQTT_TOPIC);
    client.subscribe(process.env.MQTT_TOPIC_DEVICE_CONNECTIVITY);
  });

  client.on("message", async (topic, message) => {
    let data;
    try {
      data = JSON.parse(message.toString());
      console.log(data);
    } catch (e) {
      console.error("Invalid JSON received:", e.message);
      return;
    }

    const deviceId = data.mac;
    if (!deviceId) return;

    // const deviceTime = data.time 
    //   ? new Date(data.time * 1000).toISOString() 
    //   : new Date().toISOString();



const dateObj = data.time ? new Date(data.time * 1000) : new Date();

dateObj.setHours(dateObj.getHours() - 6);

const deviceTime = dateObj.toISOString();


    const handler = getHandlerForModel(data);

    if (!handler) {
      const conn = await db.connect();
      try {
        await conn.query("BEGIN");
        await insertRawAndHeartbeat(conn, data, deviceId, deviceTime);
        await conn.query("COMMIT");
        console.log("Connectivity data committed:", deviceId);
      } catch (err) {
        await conn.query("ROLLBACK");
        console.error("DB Error:", err.message);
      } finally {
        conn.release();
      }
      return;
    }

    const conn = await db.connect();
    try {
      await conn.query("BEGIN");
      await insertRawAndHeartbeat(conn, data, deviceId, deviceTime);
      const changedSensors = await handler(conn, data, deviceId, deviceTime);
      await conn.query("COMMIT");
      console.log("Data inserted & committed:", deviceId);

      const hasChanges = Object.keys(changedSensors).length > 0;
      const hasConnectivity = data.Connectivity === "online" || data.Connectivity === "offline";

      if (hasChanges || hasConnectivity) {
        await redis.publish("device_updates", JSON.stringify({
          device_id: deviceId,
          model: data.mdl || "dems",
          time: deviceTime, 
          ...changedSensors,
          connectivity: data.Connectivity || null
        }));
      }

      if (hasConnectivity) {
        await redis.hset(`device:last:${deviceId}`, "connectivity", data.Connectivity);
      }

    } catch (err) {
      await conn.query("ROLLBACK");
      console.error("=== MQTT/DB ERROR ===");
      console.error("Message:", err.message);
      if (err.code) console.error("Code:", err.code);
      if (err.detail) console.error("Detail:", err.detail);
      console.error("=====================");
    } finally {
      conn.release();
    }
  });
}

module.exports = { startMQTT };

