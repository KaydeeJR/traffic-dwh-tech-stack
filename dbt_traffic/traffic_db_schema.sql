CREATE TABLE IF NOT EXISTS trajectories_data
(
    "track_id" INTEGER PRIMARY KEY,
    "vehicle_type" VARCHAR(50),
    "travelled_dist" FLOAT, -- in m
    "avg_speed" FLOAT, -- in km/h
    "drone_number" TEXT,
    "date" TEXT,
    "time_range" TEXT
);


CREATE TABLE IF NOT EXISTS time_frequency_data
(
    "latitude" VARCHAR(50),
    "longitude" VARCHAR(50),
    "speed" FLOAT,
    "long_acc" FLOAT,
    "lat_acc" FLOAT,
    "time" FLOAT, -- time frequency of 0.04 seconds
    "track_id" INTEGER REFERENCES trajectories_data(track_id)
    ON DELETE CASCADE
) INHERITS (trajectories_data);