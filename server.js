"use strict";

const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

/*
============================================================
 NORTHSTAR MARITIME AIS BRIDGE
 Version 3.1
 AISstream → Railway → Base44
============================================================
*/

const PORT = Number(process.env.PORT || 8080);

const AISSTREAM_URL = "wss://stream.aisstream.io/v0/stream";

const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY;
const BASE44_FUNCTION_URL = process.env.BASE44_FUNCTION_URL;
const AIS_INGEST_TOKEN = process.env.AIS_INGEST_TOKEN;

const AMHS_VESSELS = {
  "367144000": "MV Columbia",
  "338761000": "MV LeConte",
  "338205000": "MV Aurora",
  "368067220": "MV Hubbard",
  "368015640": "MV Tazlina",
  "303267000": "MV Tustumena",
  "368250000": "MV Kennicott",
  "366919560": "MV Lituya"
};

const TRACKED_MMSIS = Object.keys(AMHS_VESSELS);

let socket = null;

console.log("NorthStar Maritime AIS Bridge starting...");
app.get("/", (_req, res) => {
  res.status(200).json({
    service: "NorthStar Maritime AIS Bridge",
    version: "3.1",
    status: "running",
    aisConnected: socket?.readyState === WebSocket.OPEN,
    trackedVesselCount: TRACKED_MMSIS.length,
    trackedVessels: TRACKED_MMSIS.map((mmsi) => ({
      mmsi,
      name: AMHS_VESSELS[mmsi]
    }))
  });
});

app.get("/health", (_req, res) => {
  const connected = socket?.readyState === WebSocket.OPEN;

  res.status(connected ? 200 : 503).json({
    status: connected ? "healthy" : "degraded",
    aisConnected: connected,
    trackedVesselCount: TRACKED_MMSIS.length
  });
});

const server = app.listen(PORT, "0.0.0.0", () => {
  console.log(`Railway web server listening on port ${PORT}`);
});
function connectToAisStream() {
  if (!AISSTREAM_API_KEY) {
    console.error("Missing AISSTREAM_API_KEY");
    return;
  }

  console.log("Connecting to AISstream...");

  socket = new WebSocket(AISSTREAM_URL);

  socket.on("open", () => {
    console.log("Connected to AISstream.");

    const subscription = {
      APIKey: AISSTREAM_API_KEY,
      BoundingBoxes: [[[-90, -180], [90, 180]]],
      FiltersShipMMSI: TRACKED_MMSIS,
      FilterMessageTypes: [
        "PositionReport",
        "StandardClassBPositionReport",
        "ExtendedClassBPositionReport",
        "ShipStaticData",
        "StaticDataReport"
      ]
    };

    socket.send(JSON.stringify(subscription));

    console.log(
      `AIS subscription sent for ${TRACKED_MMSIS.length} vessels.`
    );
  });

  socket.on("message", async (rawData) => {
    try {
      const data = JSON.parse(rawData.toString());
      await processAisMessage(data);
    } catch (error) {
      console.error("AIS message error:", error.message);
    }
  });

  socket.on("error", (error) => {
    console.error("AISstream error:", error.message);
  });

  socket.on("close", (code, reasonBuffer) => {
  const reason = reasonBuffer?.toString() || "No reason provided";

  console.log(
    `AISstream disconnected. Code: ${code}. Reason: ${reason}`
  );

  console.log("Reconnecting to AISstream in 5 seconds...");

  setTimeout(() => {
    connectToAisStream();
  }, 5000);
});
 .on("close", () => {
    console.log("AISstream disconnected. Reconnecting in 5 seconds...");

    setTimeout(() => {
      connectToAisStream();
    }, 5000);
  });
}
async function processAisMessage(data) {
  if (!data.MessageType) {
    return;
  }

  const messageType = data.MessageType;
  const message = data.Message || {};
  const body = message[messageType] || {};

  const metadata =
    data.MetaData ||
    data.Metadata ||
    {};

  const mmsi = String(
    metadata.MMSI ||
    body.UserID ||
    body.MMSI ||
    ""
  );

  if (!TRACKED_MMSIS.includes(mmsi)) {
    return;
  }

  console.log("--------------------------------");
  console.log("Vessel:", AMHS_VESSELS[mmsi]);
  console.log("MMSI:", mmsi);
  console.log("Message:", messageType);

  if (
    messageType === "PositionReport" ||
    messageType === "StandardClassBPositionReport" ||
    messageType === "ExtendedClassBPositionReport"
  ) {
    await sendPositionToBase44(
      mmsi,
      body,
      metadata
    );
  }
}
async function sendPositionToBase44(
  mmsi,
  body,
  metadata
) {
  if (!BASE44_FUNCTION_URL) {
    console.error("Missing BASE44_FUNCTION_URL");
    return;
  }

  if (!AIS_INGEST_TOKEN) {
    console.error("Missing AIS_INGEST_TOKEN");
    return;
  }

  const latitude =
    body.Latitude ??
    body.latitude ??
    metadata.Latitude ??
    metadata.latitude ??
    null;

  const longitude =
    body.Longitude ??
    body.longitude ??
    metadata.Longitude ??
    metadata.longitude ??
    null;

  const speed =
    body.Sog ??
    body.SOG ??
    body.SpeedOverGround ??
    metadata.ShipSpeed ??
    null;

  const course =
    body.Cog ??
    body.COG ??
    body.CourseOverGround ??
    metadata.ShipCourse ??
    null;

  const heading =
    body.TrueHeading ??
    body.Heading ??
    metadata.TrueHeading ??
    null;

  if (latitude === null || longitude === null) {
    console.log("Position skipped because coordinates were missing.");
    return;
  }

  const payload = {
    mmsi,
    vessel_name: AMHS_VESSELS[mmsi],
    latitude: Number(latitude),
    longitude: Number(longitude),
    speed: speed === null ? null : Number(speed),
    course: course === null ? null : Number(course),
    heading: heading === null ? null : Number(heading),
    ais_report_time:
      metadata.time_utc ||
      metadata.TimeUTC ||
      metadata.Timestamp ||
      new Date().toISOString(),
    received_at: new Date().toISOString()
  };

  console.log("Sending position to Base44:", payload);

  const response = await fetch(
    BASE44_FUNCTION_URL,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-ais-ingest-token": AIS_INGEST_TOKEN
      },
      body: JSON.stringify(payload)
    }
  );

  const responseText = await response.text();

  if (!response.ok) {
    console.error(
      `Base44 error ${response.status}:`,
      responseText
    );
    return;
  }

  console.log(
    `Base44 updated ${AMHS_VESSELS[mmsi]} successfully.`
  );
}
connectToAisStream();
