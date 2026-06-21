const client = require("../config/mqtt");
const db = require("../config/db");

function startMQTT() {
  client.on("connect", () => {
    console.log("MQTT Connected");
    client.subscribe(process.env.MQTT_TOPIC);
    client.subscribe(process.env.MQTT_TOPIC_DEVICE_CONNECTIVITY);
  }); 

  client.on("message", async (topic, message) => {
    try {


const data = JSON.parse(message.toString());
const deviceId=data.mac;
const timestamp=new Date().toLocaleString("en-US", {
  timeZone: "Asia/Dhaka"
});
const deviceTime = data.time ? new Date(data.time * 1000) : new Date();


console.log(data);
// console.log(data.Connectivity);


if(!deviceId) return;


const rawData=`INSERT INTO 
dems_raw_data_logs(device_id,raw_data,device_time,created_at)
VALUES ($1, $2, $3,$4)`;

await db.query(rawData, [
  data.mac,
  JSON.stringify(data),
deviceTime,
timestamp
]);

const heartbeatData = `INSERT INTO
dems_device_heartbeat(device_mac,connectivity,created_at)
VALUES ($1, $2, $3)`;

// const conn=data.Connectivity;
// console.log("Connectivity data inserted///////////////".$conn);

if(data.Connectivity=="online"||data.Connectivity=="offline"){
await db.query(heartbeatData, [
  data.mac,
  data.Connectivity,
  timestamp
]);


}

const cleanData=`INSERT INTO 
dems_sensor_clean_logs(device_id,sensor_type,sensor_num,sensor_value,alert,device_time,created_at)
VALUES($1,$2,$3,$4,$5,$6,$7)`;


if(data.mq2!==undefined){
  await db.query(cleanData,[
    data.mac,
    "mq2",
    data.Sid,
    data.mq2,
    0,
    deviceTime,
    timestamp
  ]

  )
}

if(data.DemsHum!==undefined){
    await db.query(cleanData,[
      data.mac,
      "h",
      data.Sid,
      data.DemsHum,
      0,
   deviceTime,
   timestamp

    ])
  }


  if(data.DemsTemp!==undefined){
    await db.query(cleanData,[
      data.mac,
      "t",
      data.Sid,
      data.DemsTemp,
      0,
      deviceTime,
   timestamp

    ])
  }





/////////////////////////////////////////////////////////////////////////////////////////////////

// const cleandummyytabledata =`INSERT INTO clean_iot_data
// (device_id, temperature, humidity, timestamp)
// VALUES($1,$2,$3,$4);
// `

// await db.query(cleandummyytabledata,[
//                 deviceId,
//                 data.DemsTemp,
//                 data.DemsHum,
//                 timestamp
// ]);

//  console.log("Clean data inserted successfully");
//             console.log("Inserted Data:", {
//                 device_id:deviceId,
//                 temperature: data.DemsTemp,
//                 humidity: data.DemsHum,
//                 gas_detected: data.mq2,
//                 timestamp: timestamp
//             });




    //   const {
    //     mac: deviceId,
    //     DemsTemp,
    //     DemsHum,
    //     mq2,
    //     time
    //   } = data;

    //   if (
    //     deviceId == null ||
    //     DemsTemp == null ||
    //     DemsHum == null ||
    //     mq2 == null
//       ) {
//         console.log("⚠ Incomplete payload");
//         console.log("Received:", data);
//         return;
//       }

//       if (DemsTemp < -40 || DemsTemp > 100) return;
//       if (DemsHum < 0 || DemsHum > 100) return;
// //////////////////////////
//       const timestamp = time
//         ? new Date(time * 1000)
//         : new Date();
// ////////////

// cleanData=`INSERT INTO dems_sensor_data(device_id,sensor_type,sensor_num,sensor_value,alert,insert_time)
// VALUES ($1, $2, $3, $4, $5, $6)`;

// await db.query(cleanData,[
//     deviceId,
//     sensor_type="t",
//     sensor_num=1
// ])



//       const sql = `
//         INSERT INTO clean_iot_data
//         (device_id, temperature, humidity, gas_detected, timestamp)
//         VALUES ($1, $2, $3, $4, $5)
//       `;

//       await db.query(sql, [
//         deviceId,
//         DemsTemp,
//         DemsHum,
//         mq2,
//         timestamp
//       ]);
      console.log(" Data inserted:", deviceId);

    } catch (err) {
      console.error("MQTT ERROR:", err.message);
    }
  });
}

module.exports = { startMQTT };
