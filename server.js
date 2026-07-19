
const express = require("express");
const WebSocket = require("ws");
const axios = require("axios");

const app = express();

const PORT = process.env.PORT || 3000;

const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY;
const AIS_INGEST_TOKEN = process.env.AIS_INGEST_TOKEN;
const BASE44_FUNCTION_URL = process.env.BASE44_FUNCTION_URL;

const trackedMmsis = (process.env.VESSEL_MMSI_LIST || "")
  .split(",")
  .map((mmsi) => mmsi.trim())
  .filter(Boolean);

let socket = null;
let reconnectTimer = null;
let heartbeatTimer = null;
let reconnectAttempts = 0;
let lastAisMessageTime = null;
let lastPongTime = null;

const HEARTBEAT_INTERVAL_MS = 30000;
const PONG_TIMEOUT_MS = 90000;
const MAX_RECONNECT_DELAY_MS = 60000;

function validateEnvironmentVariables() {
  const missingVariables = [];

  if (!AISSTREAM_API_KEY) {
    missingVariables.push("AISSTREAM_API_KEY");
  }

  if (!AIS_INGEST_TOKEN) {
    missingVariables.push("AIS_INGEST_TOKEN");
  }

  if (!BASE44_FUNCTION_URL) {
    missingVariables.push("BASE44_FUNCTION_URL");
  }

  if (trackedMmsis.length === 0) {
    missingVariables.push("VESSEL_MMSI_LIST");
  }

  if (missingVariables.length > 0) {
    console.error(
      `Missing Railway variables: ${missingVariables.join(", ")}`
    );

    return false;
  }

  return true;
}

function clearHeartbeatTimer() {
  if (heartbeatTimer) {
    clearInterval(heartbeatTimer);
    heartbeatTimer = null;
  }
}

function clearReconnectTimer() {
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
}

function scheduleReconnect(reason) {
  clearHeartbeatTimer();

  if (reconnectTimer) {
    return;
  }

  reconnectAttempts += 1;

  const reconnectDelay = Math.min(
    5000 * Math.pow(2, reconnectAttempts - 1),
    MAX_RECONNECT_DELAY_MS
  );

  console.log(
    `AISstream disconnected: ${reason}. Reconnecting in ${
      reconnectDelay / 1000
    } seconds.`
  );

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    connectToAisstream();
  }, reconnectDelay);
}

function isValidCoordinate(latitude, longitude) {
  return (
    Number.isFinite(latitude) &&
    Number.isFinite(longitude) &&
    latitude >= -90 &&
    latitude <= 90 &&
    longitude >= -180 &&
    longitude <= 180 &&
    latitude !== 91 &&
    longitude !== 181
  );
}

function getAisTimestamp(metadata) {
  const rawTimestamp =
    metadata?.time_utc ||
    metadata?.TimeUTC ||
    metadata?.timestamp ||
    metadata?.Timestamp;

  if (!rawTimestamp) {
    return new Date().toISOString();
  }

  const parsedDate = new Date(rawTimestamp);

  if (Number.isNaN(parsedDate.getTime())) {
    return new Date().toISOString();
  }

  return parsedDate.toISOString();
}

async function sendToBase44(payload, description) {
  try {
    const response = await axios.post(BASE44_FUNCTION_URL, payload, {
      headers: {
        "Content-Type": "application/json",
        "x-ais-ingest-token": AIS_INGEST_TOKEN
      },
      timeout: 15000
    });

    console.log(
      `${description} successfully sent to Base44:`,
      JSON.stringify(response.data)
    );
  } catch (error) {
    const status = error.response?.status;
    const responseData = error.response?.data;

    console.error(
      `${description} failed:`,
      status || error.message,
      responseData || ""
    );
  }
}

async function processPositionReport(message) {
  const metadata = message.MetaData || {};
  const positionReport = message.Message?.PositionReport;

  if (!positionReport) {
    return;
  }

  const mmsi = String(metadata.MMSI || "").trim();

  if (!mmsi) {
    console.log("Position report ignored because MMSI was missing.");
    return;
  }

  const latitude = Number(positionReport.Latitude);
  const longitude = Number(positionReport.Longitude);

  if (!isValidCoordinate(latitude, longitude)) {
    console.log(`Invalid coordinates ignored for MMSI ${mmsi}.`, {
      latitude: positionReport.Latitude,
      longitude: positionReport.Longitude
    });

    return;
  }

  const speed = Number(positionReport.Sog);
  const course = Number(positionReport.Cog);
  const heading = Number(positionReport.TrueHeading);

  const payload = {
    message_type: "PositionReport",
    mmsi,
    vessel_name: metadata.ShipName || null,
    latitude,
    longitude,

    speed: Number.isFinite(speed) ? speed : null,
    speed_over_ground: Number.isFinite(speed) ? speed : null,

    course: Number.isFinite(course) ? course : null,
    course_over_ground: Number.isFinite(course) ? course : null,

    heading: Number.isFinite(heading) ? heading : null,
    true_heading: Number.isFinite(heading) ? heading : null,

    navigation_status:
      positionReport.NavigationalStatus !== undefined
        ? positionReport.NavigationalStatus
        : null,

    last_updated: new Date().toISOString(),
    ais_report_time: getAisTimestamp(metadata)
  };

  console.log("AIS position received:", {
    mmsi,
    latitude,
    longitude,
    speed: payload.speed,
    course: payload.course,
    heading: payload.heading,
    ais_report_time: payload.ais_report_time
  });

  await sendToBase44(
    payload,
    `Position update for vessel ${mmsi}`
  );
}

async function processShipStaticData(message) {
  const metadata = message.MetaData || {};
  const staticData = message.Message?.ShipStaticData;

  if (!staticData) {
    return;
  }

  const mmsi = String(metadata.MMSI || "").trim();

  if (!mmsi) {
    console.log("Static-data report ignored because MMSI was missing.");
    return;
  }

  const payload = {
    message_type: "ShipStaticData",
    mmsi,

    vessel_name:
      staticData.Name ||
      staticData.ShipName ||
      metadata.ShipName ||
      null,

    call_sign:
      staticData.CallSign ||
      staticData.Callsign ||
      null,

    destination: staticData.Destination || null,

    eta:
      staticData.Eta ||
      staticData.ETA ||
      null,

    ship_type:
      staticData.Type ||
      staticData.ShipType ||
      null,

    last_updated: new Date().toISOString(),
    ais_report_time: getAisTimestamp(metadata)
  };

  console.log("AIS static data received:", {
    mmsi,
    vessel_name: payload.vessel_name,
    destination: payload.destination,
    call_sign: payload.call_sign
  });

  await sendToBase44(
    payload,
    `Static-data update for vessel ${mmsi}`
  );
}

function startHeartbeat() {
  clearHeartbeatTimer();

  lastPongTime = Date.now();

  heartbeatTimer = setInterval(() => {
    if (!socket) {
      scheduleReconnect("socket does not exist");
      return;
    }

    if (socket.readyState !== WebSocket.OPEN) {
      scheduleReconnect("socket is not open");
      return;
    }

    const millisecondsSinceLastPong = Date.now() - lastPongTime;

    if (millisecondsSinceLastPong > PONG_TIMEOUT_MS) {
      console.log(
        "AISstream did not respond to heartbeat. Restarting connection."
      );

      socket.terminate();
      return;
    }

    try {
      socket.ping();
      console.log("AISstream heartbeat sent.");
    } catch (error) {
      console.error(
        "Could not send AISstream heartbeat:",
        error.message
      );

      socket.terminate();
    }
  }, HEARTBEAT_INTERVAL_MS);
}

function connectToAisstream() {
  if (!validateEnvironmentVariables()) {
    return;
  }

  if (
    socket &&
    (
      socket.readyState === WebSocket.OPEN ||
      socket.readyState === WebSocket.CONNECTING
    )
  ) {
    console.log("AISstream connection already exists.");
    return;
  }

  clearReconnectTimer();

  console.log("Connecting to AISstream...");

  socket = new WebSocket(
    "wss://stream.aisstream.io/v0/stream"
  );

  socket.on("open", () => {
    reconnectAttempts = 0;
    lastPongTime = Date.now();

    console.log("Connected to AISstream.");

    const subscription = {
      APIKey: AISSTREAM_API_KEY,

      BoundingBoxes: [
        [
          [50, -180],
          [72, -120]
        ]
      ],

      FiltersShipMMSI: trackedMmsis,

      FilterMessageTypes: [
        "PositionReport",
        "ShipStaticData"
      ]
    };

    socket.send(JSON.stringify(subscription));

    console.log(
      `AIS subscription sent for ${trackedMmsis.length} vessel(s): ${trackedMmsis.join(
        ", "
      )}`
    );

    startHeartbeat();
  });

  socket.on("message", async (data) => {
    lastAisMessageTime = Date.now();

    try {
      const message = JSON.parse(data.toString());

      const messageType = message.MessageType;
      const mmsi = message.MetaData?.MMSI;

      console.log(
        `AIS message received: ${messageType} ${mmsi || ""}`
      );

      if (messageType === "PositionReport") {
        await processPositionReport(message);
        return;
      }

      if (messageType === "ShipStaticData") {
        await processShipStaticData(message);
      }
    } catch (error) {
      console.error(
        "AIS message could not be processed:",
        error.message
      );
    }
  });

  socket.on("pong", () => {
    lastPongTime = Date.now();
    console.log("AISstream heartbeat response received.");
  });

  socket.on("close", (code, reasonBuffer) => {
    const reason =
      reasonBuffer?.toString() || "No reason provided";

    console.log(
      `AISstream connection closed. Code: ${code}. Reason: ${reason}`
    );

    clearHeartbeatTimer();
    socket = null;

    scheduleReconnect(`connection closed with code ${code}`);
  });

  socket.on("error", (error) => {
    console.error(
      "AISstream connection error:",
      error.message
    );

    if (socket) {
      socket.terminate();
    }
  });
}

app.get("/", (req, res) => {
  const connectionStates = {
    [WebSocket.CONNECTING]: "connecting",
    [WebSocket.OPEN]: "connected",
    [WebSocket.CLOSING]: "closing",
    [WebSocket.CLOSED]: "disconnected"
  };

  res.json({
    status: "running",

    aisConnection:
      socket
        ? connectionStates[socket.readyState]
        : "disconnected",

    trackedVessels: trackedMmsis.length,
    trackedMmsis,

    lastAisMessageTime:
      lastAisMessageTime
        ? new Date(lastAisMessageTime).toISOString()
        : null,

    lastHeartbeatResponse:
      lastPongTime
        ? new Date(lastPongTime).toISOString()
        : null,

    reconnectAttempts
  });
});

app.listen(PORT, () => {
  console.log(`Railway health server running on port ${PORT}.`);
  connectToAisstream();
});
