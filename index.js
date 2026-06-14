app.get("/api/latest", (req, res) => {

    const sql = `
        SELECT * FROM iot_sensor_data
        ORDER BY id DESC
        LIMIT 1
    `;

    db.query(sql, (err, result) => {

        if (err) {
            return res.json({ error: err });
        }

        res.json(result[0]);
        // returns latest row as JSON
    });

});