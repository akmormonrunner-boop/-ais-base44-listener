socket.on("message", async (data) => {
  try {
    const message = JSON.parse(data.toString());

    // TEMPORARY DEBUG LINE
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

    console.log(`Updated vessel ${mmsi} in Base44.`);
  } catch (error) {
    console.error("Failed to process AIS message:", error);
  }
});
