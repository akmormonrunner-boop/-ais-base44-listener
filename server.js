const express = require("express");
const WebSocket = require("ws");

const app = express();
const port = process.env.PORT || 3000;

const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY;
const BASE44_ENDPOINT_URL = process.env.BASE44_ENDPOINT_URL;
const AIS_INGEST_TOKEN = process.env.AIS_INGEST_TOKEN;

const vesselMmsiNumbers = (process.env.VESSEL_MMSI_LIST || "")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);

if (!AISSTREAM_API_KEY) {
  throw new Error("AISSTREAM_API_KEY is missing.");
}

if (!BASE44_ENDPOINT_URL) {
  throw new Error("BASE44_ENDPOINT_URL is missing.");
}

if (!AIS_INGEST_TOKEN) {
  throw new Error("AIS_INGEST_TOKEN is missing.");
}

if (vesselMmsiNumbers.length === 0) {
  throw new Error("VESSEL_MMSI_LIST is empty.");
}

app.get("/", (req, res) => {
  res.json({
    running: true,
    trackedVessels: vesselMmsiNumbers.length
  });
});

app.listen(port, () => {
  console.log(`Health-check server listening on port ${port}.`);
});

let socket;
let reconnectTimer;

function connectToAisStream() {
  console.log("Connecting to AISstream...");

  socket = new WebSocket("wss://stream.aisstream.io/v0/stream");

  socket.on("open", () => {
    console.log("Connected to AISstream.");

    socket.send(
      JSON.stringify({
        APIKey: AISSTREAM_API_KEY,
        BoundingBoxes: [
          [
            [48, -180],
            [72, -125]
          ]
        ],
        FiltersShipMMSI: vesselMmsiNumbers,
        FilterMessageTypes: ["PositionReport"]
      })
    );
  });

  socket.on("message", async (data) => {
    try {
      const message = JSON.parse(data.toString());

      console.log(
        "AIS message received:",
        message.MessageType,
        message.MetaData?.MMSI
      );

      if (message.MessageType !== "PositionReport") {
        return;
      }

      const metadata = message.MetaData || {};
      const position = message.Message?.PositionReport || {};

      const mmsi = String(metadata.MMSI || position.UserID || "");

      if (!vesselMmsiNumbers.includes(mmsi)) {
        return;
      }

      const latitude = position.Latitude;
      const longitude = position.Longitude;

      if (
        typeof latitude !== "number" ||
        latitude < -90 ||
        latitude > 90 ||
        typeof longitude !== "number" ||
        longitude < -180 ||
        longitude > 180
      ) {
        console.warn(`Invalid position received for ${mmsi}.`);
        return;
      }

      const update = {
        mmsi,
        latitude,
        longitude,
        speed_over_ground: position.Sog ?? null,
        course_over_ground: position.Cog ?? null,
        true_heading: position.TrueHeading ?? null,
        navigation_status: position.NavigationalStatus ?? null,
        destination: null,
        ais_report_time: metadata.time_utc || new Date().toISOString()
      };

      console.log(`Sending update for vessel ${mmsi} to Base44...`);

      const response = await fetch(BASE44_ENDPOINT_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-ais-ingest-token": AIS_INGEST_TOKEN
        },
        body: JSON.stringify(update)
      });

      if (!response.ok) {
        const errorText = await response.text();

        console.error(
          `Base44 update failed for ${mmsi}: ${response.status} ${errorText}`
        );

        return;
      }

      const responseText = await response.text();

      console.log(
        `Updated vessel ${mmsi} in Base44.${responseText ? ` ${responseText}` : ""}`
      );
    } catch (error) {
      console.error("Failed to process AIS message:", error.message);
    }
  });

  socket.on("error", (error) => {
    console.error("AISstream WebSocket error:", error.message);
  });

  socket.on("close", () => {
    console.warn("AISstream disconnected. Reconnecting in 10 seconds.");

    clearTimeout(reconnectTimer);
    reconnectTimer = setTimeout(connectToAisStream, 10000);
  });
}

connectToAisStream();
