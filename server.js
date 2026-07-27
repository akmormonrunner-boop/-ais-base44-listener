"use strict";

const express = require("express");
const WebSocket = require("ws");

const app = express();
app.use(express.json());

/*
============================================================
 NORTHSTAR MARITIME AIS BRIDGE
 Version 3.2
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

const NAVIGATION_STATUS = {
  0: "Underway Using Engine",
  1: "At Anchor",
  2: "Not Under Command",
  3: "Restricted Maneuverability",
  4: "Constrained by Draft",
  5: "Moored",
  6: "Aground",
  7: "Engaged in Fishing",
  8: "Under Sail",
  9: "Reserved",
  10: "Reserved",
  11: "Reserved",
  12: "Reserved",
  13: "Reserved",
  14: "AIS-SART",
  15: "Unknown"
};

const TRACKED_MMSIS = Object.keys(AMHS_VESSELS);
const vesselDestinations = {};

let socket = null;

console.log("======================================");
console.log("NorthStar Maritime AIS Bridge");
console.log("Version 3.2");
console.log("======================================");

app.get("/", (_req, res) => {
  res.status(200).json({
    service: "NorthStar Maritime AIS Bridge",
    version: "3.2",
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

  socket = new WebSocket(AISSTREAM_URL, {
    handshakeTimeout: 30000
  });

  socket.on("open", () => {
    console.log("Connected to AISstream.");


const subscription = {
  APIKey: AISSTREAM_API_KEY,

  BoundingBoxes: [
    [
      [47.0, -180.0],
      [61.8, -122.0]
    ]
  ]
};

  
    socket.send(JSON.stringify(subscription));
   console.log("Subscription sent to AISstream.");

    console.log(
      `AIS subscription sent for ${TRACKED_MMSIS.length} vessels.`
    );
  });
  

 socket.on("message", async (rawData) => {
  try {
    const data = JSON.parse(rawData.toString());
    await processAisMessage(data);
  } catch (error) {
    console.error(
      "AIS message processing error:",
      error.message
    );
  }
});


  socket.on("error", (error) => {
    console.error("========== AISSTREAM ERROR ==========");
    console.error("Message:", error.message);
    console.error("Code:", error.code);
    console.error("Name:", error.name);
    console.error(error);
    console.error("=====================================");
  });

  socket.on("close", (code, reasonBuffer) => {
    const reason =
      reasonBuffer && reasonBuffer.length
        ? reasonBuffer.toString()
        : "No reason provided";

    console.log(
      `AISstream disconnected. Code: ${code}. Reason: ${reason}`
    );

    console.log("Reconnecting in 5 seconds...");

    setTimeout(() => {
      connectToAisStream();
    }, 5000);
  });
}

async function processAisMessage(data) {
  if (!data || !data.MessageType) {
    return;
  }

  const messageType = data.MessageType;

  const message = data.Message || {};

  const body = message[messageType] || {};

  const metadata =
    data.MetaData ||
    data.Metadata ||
    {};

if (
  messageType === "ShipStaticData" ||
  messageType === "StaticDataReport"
) {
  const staticMmsi = String(
    metadata.MMSI ||
    body.UserID ||
    body.MMSI ||
    ""
  );

  const destination = String(
    body.Destination ||
    body.destination ||
    ""
  ).trim();

  if (
    TRACKED_MMSIS.includes(staticMmsi) &&
    destination
  ) {
    vesselDestinations[staticMmsi] = destination;

    console.log("--------------------------------");
    console.log("AMHS AIS Destination Updated");
    console.log("Vessel:", AMHS_VESSELS[staticMmsi]);
    console.log("MMSI:", staticMmsi);
    console.log("Destination:", destination);
    console.log("--------------------------------");
  }
}
 
  const mmsi = String(
    metadata.MMSI ||
    body.UserID ||
    body.MMSI ||
    ""
  );

  const vesselName =
  metadata.ShipName ||
  metadata.shipName ||
  body.Name ||
  body.ShipName ||
  "Unknown";

console.log("--------------------------------");
console.log("AIS Message Received");
console.log("MMSI:", mmsi);
console.log("Reported Name:", vesselName);

if (!TRACKED_MMSIS.includes(mmsi)) {
  console.log("Not an AMHS vessel");
  return;
}

  console.log("--------------------------------");
  console.log("Vessel:", AMHS_VESSELS[mmsi]);
  console.log("MMSI:", mmsi);
  console.log("Message Type:", messageType);

  switch (messageType) {
    case "PositionReport":
    case "StandardClassBPositionReport":
    case "ExtendedClassBPositionReport":
      await sendPositionToBase44(
        mmsi,
        body,
        metadata
      );
      break;

    case "ShipStaticData":
    case "StaticDataReport":
      console.log(
        `${AMHS_VESSELS[mmsi]} static data received.`
      );
      break;

    default:
      console.log(
        `Unhandled AIS message: ${messageType}`
      );
      break;
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

 const navigationStatusCode =
  body.NavigationalStatus ??
  body.NavigationStatus ??
  body.NavStatus ??
  metadata.NavigationalStatus ??
  metadata.NavigationStatus ??
  null;

const navigationStatus =
  navigationStatusCode === null
    ? "Unknown"
    : (NAVIGATION_STATUS[navigationStatusCode] || "Unknown");

  if (latitude === null || longitude === null) {
    console.log(
      "Position skipped because coordinates were missing."
    );
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
   navigation_status: navigationStatus,
  navigation_status_code: navigationStatusCode,
    ais_report_time:
      metadata.time_utc ||
      metadata.TimeUTC ||
      metadata.Timestamp ||
      new Date().toISOString(),
    received_at: new Date().toISOString()
  };

  console.log("--------------------------------");
  console.log("Sending position to Base44");
  console.log(payload);

   try {
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
        `Base44 returned ${response.status}`
      );
      console.error(responseText);
      return;
    }

    console.log(
      `✓ ${AMHS_VESSELS[mmsi]} updated successfully.`
    );
  } catch (error) {
    console.error(
      "Failed sending to Base44:"
    );
    console.error(error);
  }
}

function logStartupInformation() {
  console.log("======================================");
  console.log("NorthStar Maritime AIS Bridge Ready");
  console.log("======================================");
  console.log(`Tracked vessels: ${TRACKED_MMSIS.length}`);

  TRACKED_MMSIS.forEach((mmsi) => {
    console.log(`• ${AMHS_VESSELS[mmsi]} (${mmsi})`);
  });

  console.log("--------------------------------------");

  if (!AISSTREAM_API_KEY) {
    console.error("ERROR: AISSTREAM_API_KEY is missing.");
  } else {
    console.log("AISSTREAM_API_KEY loaded.");
  }

  if (!BASE44_FUNCTION_URL) {
    console.error("ERROR: BASE44_FUNCTION_URL is missing.");
  } else {
    console.log("BASE44_FUNCTION_URL loaded.");
  }

  if (!AIS_INGEST_TOKEN) {
    console.error("ERROR: AIS_INGEST_TOKEN is missing.");
  } else {
    console.log("AIS_INGEST_TOKEN loaded.");
  }

  console.log("======================================");
}

function shutdown() {
  console.log("--------------------------------------");
  console.log("NorthStar Maritime shutting down...");

  if (socket) {
    try {
      socket.close();
    } catch (error) {
      console.error("Error closing WebSocket:", error);
    }
  }

  server.close(() => {
    console.log("HTTP server closed.");
    process.exit(0);
  });

  setTimeout(() => {
    console.log("Force exiting process.");
    process.exit(1);
  }, 5000);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

process.on("uncaughtException", (error) => {
  console.error("========== UNCAUGHT EXCEPTION ==========");
  console.error(error);
  console.error("========================================");
});

process.on("unhandledRejection", (reason) => {
  console.error("========== UNHANDLED REJECTION ==========");
  console.error(reason);
  console.error("=========================================");
});

/*
============================================================
 START NORTHSTAR AIS BRIDGE
============================================================
*/

logStartupInformation();

connectToAisStream();

/*
============================================================
 KEEP RAILWAY SERVICE ALIVE
============================================================
*/

process.on("exit", (code) => {
  console.log(`Process exited with code ${code}`);
});

setInterval(() => {
  const connected = socket?.readyState === WebSocket.OPEN;

  console.log("--------------------------------------");
  console.log(
    `[${new Date().toISOString()}] Bridge Status`
  );
  console.log(
    `AIS Connected: ${connected ? "YES" : "NO"}`
  );
  console.log(
    `Tracking ${TRACKED_MMSIS.length} vessels`
  );
  console.log("--------------------------------------");
}, 60000);

