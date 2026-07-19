"use strict";

const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

/*
 * ============================================================
 * NORTHSTAR MARITIME AIS BRIDGE — VERSION 3
 *
 * AISstream → Railway → Base44
 * ============================================================
 */

const PORT = Number(process.env.PORT || 8080);

const AISSTREAM_API_KEY = process.env.AISSTREAM_API_KEY;
const BASE44_FUNCTION_URL = process.env.BASE44_FUNCTION_URL;
const AIS_INGEST_TOKEN = process.env.AIS_INGEST_TOKEN;

/*
 * AMHS fleet MMSIs
 *
 * These MMSIs are included directly in the code.
 * You no longer need the Railway VESSEL_MMSI_LIST variable.
 */
const AMHS_VESSELS = Object.freeze({
  "367144000": {
    name: "MV Columbia"
  },
  "338761000": {
    name: "MV LeConte"
  },
  "338205000": {
    name: "MV Aurora"
  },
  "368067220": {
    name: "MV Hubbard"
  },
  "368015640": {
    name: "MV Tazlina"
  },
  "303267000": {
    name: "MV Tustumena"
  },
  "368250000": {
    name: "MV Kennicott"
  },
  "366919560": {
    name: "MV Lituya"
  }
});

const vesselMmsiList = Object.keys(AMHS_VESSELS);

/*
 * AISstream configuration
 */
const AISSTREAM_URL =
  "wss://stream.aisstream.io/v0/stream";

const AIS_MESSAGE_TYPES = [
  "PositionReport",
  "StandardClassBPositionReport",
  "ExtendedClassBPositionReport",
  "ShipStaticData",
  "StaticDataReport"
];

/*
 * Timing configuration
 */
const HEARTBEAT_INTERVAL_MS = 30_000;
const MAX_RECONNECT_DELAY_MS = 60_000;
const BASE44_REQUEST_TIMEOUT_MS = 15_000;
const BASE44_RETRY_ATTEMPTS = 3;

/*
 * Runtime state
 */
let aisSocket = null;
let heartbeatTimer = null;
let reconnectTimer = null;
let reconnectAttempts = 0;
let pongReceived = true;
let shuttingDown = false;

/*
 * Prevent sending identical position messages repeatedly.
 */
const lastPositionByMmsi = new Map();

/*
 * Service statistics
 */
const stats = {
  serviceStartedAt: new Date().toISOString(),
  websocketConnections: 0,
  websocketReconnects: 0,
  messagesReceived: 0,
  positionMessagesReceived: 0,
  staticMessagesReceived: 0,
  messagesIgnored: 0,
  base44SuccessfulUpdates: 0,
  base44FailedUpdates: 0,
  duplicatePositionsSkipped: 0,
  lastAisMessageAt: null,
  lastPositionAt: null,
  lastBase44SuccessAt: null,
  lastWebSocketOpenAt: null,
  lastWebSocketCloseAt: null,
  lastError: null
};

/*
 * ============================================================
 * EXPRESS ENDPOINTS
 * ============================================================
 */

app.get("/", (req, res) => {
  res.status(200).json({
    service: "NorthStar Maritime AIS Bridge",
    version: "3.0.0",
    status: "running",
    aisConnected:
      aisSocket?.readyState === WebSocket.OPEN,
    trackedVesselCount: vesselMmsiList.length,
    trackedVessels: AMHS_VESSELS,
    serviceStartedAt: stats.serviceStartedAt
  });
});

app.get("/health", (req, res) => {
  const connected =
    aisSocket?.readyState === WebSocket.OPEN;

  res.status(connected ? 200 : 503).json({
    status: connected ? "healthy" : "degraded",
    aisConnected: connected,
    trackedVesselCount: vesselMmsiList.length,
    lastAisMessageAt: stats.lastAisMessageAt,
    lastBase44SuccessAt: stats.lastBase44SuccessAt
  });
});

app.get("/stats", (req, res) => {
  res.status(200).json({
    ...stats,
    aisConnected:
      aisSocket?.readyState === WebSocket.OPEN,
    reconnectAttempts,
    trackedVessels: vesselMmsiList.map((mmsi) => ({
      mmsi,
      name: AMHS_VESSELS[mmsi].name,
      lastPosition:
        lastPositionByMmsi.get(mmsi) || null
    }))
  });
});

/*
 * Start Railway's web server first.
 */
app.listen(PORT, "0.0.0.0", () => {
  console.log("================================================");
  console.log("NorthStar Maritime AIS Bridge Version 3");
  console.log(`Railway web server running on port ${PORT}.`);
  console.log("================================================");

  const missingVariables = getMissingVariables();

  if (missingVariables.length > 0) {
    const errorMessage =
      `Missing Railway variables: ${missingVariables.join(", ")}`;

    stats.lastError = errorMessage;
    console.error(errorMessage);
    return;
  }

  console.log(`Tracking ${vesselMmsiList.length} AMHS vessels:`);

  for (const mmsi of vesselMmsiList) {
    console.log(
      `- ${AMHS_VESSELS[mmsi].name}: ${mmsi}`
    );
  }

  connectToAISStream();
});

/*
 * ============================================================
 * AISSTREAM CONNECTION
 * ============================================================
 */

function connectToAISStream() {
  if (shuttingDown) {
    return;
  }

  if (
    aisSocket &&
    (
      aisSocket.readyState === WebSocket.OPEN ||
      aisSocket.readyState === WebSocket.CONNECTING
    )
  ) {
    console.log(
      "AISstream connection already open or connecting."
    );
    return;
  }

  clearReconnectTimer();
  stopHeartbeat();

  console.log("Connecting to AISstream...");

  const socket = new WebSocket(AISSTREAM_URL);
  aisSocket = socket;

  socket.on("open", () => {
    if (socket !== aisSocket) {
      return;
    }

    reconnectAttempts = 0;
    pongReceived = true;

    stats.websocketConnections += 1;
    stats.lastWebSocketOpenAt =
      new Date().toISOString();

    console.log("Connected to AISstream.");

    /*
     * AISstream requires a subscription message shortly
     * after the WebSocket opens.
     */
    const subscription = {
      APIKey: AISSTREAM_API_KEY,

      /*
       * Worldwide bounding box.
       * The MMSI list limits the subscription to AMHS vessels.
       */
      BoundingBoxes: [
        [
          [-90, -180],
          [90, 180]
        ]
      ],

      FiltersShipMMSI: vesselMmsiList,

      FilterMessageTypes: AIS_MESSAGE_TYPES
    };

    try {
      socket.send(JSON.stringify(subscription));

      console.log(
        `AIS subscription sent for ${vesselMmsiList.length} vessels:`
      );

      for (const mmsi of vesselMmsiList) {
        console.log(
          `- ${AMHS_VESSELS[mmsi].name}: ${mmsi}`
        );
      }

      console.log(
        `AIS message types: ${AIS_MESSAGE_TYPES.join(", ")}`
      );

      startHeartbeat(socket);
    } catch (error) {
      recordError(
        "Unable to send AIS subscription",
        error
      );

      socket.terminate();
    }
  });

  socket.on("message", async (rawData) => {
    if (socket !== aisSocket) {
      return;
    }

    await processAISMessage(rawData);
  });

  socket.on("pong", () => {
    if (socket !== aisSocket) {
      return;
    }

    pongReceived = true;
    console.log(
      "AISstream heartbeat response received."
    );
  });

  socket.on("error", (error) => {
    if (socket !== aisSocket) {
      return;
    }

    recordError(
      "AISstream WebSocket error",
      error
    );
  });

  socket.on("close", (code, reasonBuffer) => {
    if (socket !== aisSocket) {
      return;
    }

    stopHeartbeat();

    const reason =
      reasonBuffer?.toString() ||
      "No reason provided";

    stats.lastWebSocketCloseAt =
      new Date().toISOString();

    console.log(
      `AISstream connection closed. Code: ${code}. Reason: ${reason}`
    );

    aisSocket = null;

    if (!shuttingDown) {
      scheduleReconnect();
    }
  });
}

/*
 * ============================================================
 * AIS MESSAGE PROCESSING
 * ============================================================
 */

async function processAISMessage(rawData) {
  let aisData;

  try {
    aisData = JSON.parse(rawData.toString());
  } catch (error) {
    stats.messagesIgnored += 1;

    recordError(
      "AISstream sent invalid JSON",
      error
    );

    return;
  }

  /*
   * AISstream may return errors through the WebSocket,
   * for example an invalid API key or subscription.
   */
  if (aisData.error) {
    const errorMessage =
      `AISstream subscription error: ${aisData.error}`;

    stats.lastError = errorMessage;
    console.error(errorMessage);
    return;
  }

  stats.messagesReceived += 1;
  stats.lastAisMessageAt =
    new Date().toISOString();

  const messageType =
    String(aisData.MessageType || "").trim();

  const metadata =
    aisData.MetaData ||
    aisData.Metadata ||
    {};

  /*
   * AISstream places the actual message body under:
   *
   * Message.PositionReport
   * Message.ShipStaticData
   * Message.StandardClassBPositionReport
   * etc.
   */
  const messageContainer =
    aisData.Message || {};

  const messageBody =
    messageContainer[messageType] ||
    messageContainer;

  const mmsi = extractMmsi(
    metadata,
    messageBody
  );

  console.log(
    `AIS message received: ${
      messageType || "Unknown"
    }${mmsi ? ` | MMSI ${mmsi}` : ""}`
  );

  if (!messageType) {
    stats.messagesIgnored += 1;
    console.log(
      "AIS message ignored because MessageType was missing."
    );
    return;
  }

  if (!mmsi) {
    stats.messagesIgnored += 1;
    console.log(
      "AIS message ignored because no MMSI was found."
    );
    return;
  }

  if (!AMHS_VESSELS[mmsi]) {
    stats.messagesIgnored += 1;
    console.log(
      `Ignoring untracked MMSI ${mmsi}.`
    );
    return;
  }

  switch (messageType) {
    case "PositionReport":
    case "StandardClassBPositionReport":
    case "ExtendedClassBPositionReport":
      stats.positionMessagesReceived += 1;

      await handlePositionReport({
        messageType,
        mmsi,
        metadata,
        messageBody
      });

      break;

    case "ShipStaticData":
    case "StaticDataReport":
      stats.staticMessagesReceived += 1;

      await handleStaticData({
        messageType,
        mmsi,
        metadata,
        messageBody
      });

      break;

    default:
      stats.messagesIgnored += 1;

      console.log(
        `No handler configured for ${messageType}.`
      );
  }
}

/*
 * ============================================================
 * POSITION REPORTS
 * ============================================================
 */

async function handlePositionReport({
  messageType,
  mmsi,
  metadata,
  messageBody
}) {
  const vessel =
    AMHS_VESSELS[mmsi];

  const latitude = firstValidNumber([
    messageBody.Latitude,
    messageBody.latitude,
    metadata.Latitude,
    metadata.latitude
  ]);

  const longitude = firstValidNumber([
    messageBody.Longitude,
    messageBody.longitude,
    metadata.Longitude,
    metadata.longitude
  ]);

  const speed = normalizeAisNumber(
    firstValidNumber([
      messageBody.Sog,
      messageBody.SOG,
      messageBody.SpeedOverGround,
      metadata.ShipSpeed
