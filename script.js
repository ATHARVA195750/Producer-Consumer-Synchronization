/* ============================================================
   Fire Alarm Monitoring System — script.js
   Operating Systems Practical: Producer – Consumer Problem
   ------------------------------------------------------------
   Producers : Smoke, Heat and Flame sensors generate alerts.
   Buffer    : A bounded FIFO queue (maximum 5 pending alerts).
   Consumers : Two simulated emergency-response workers.
   Sync      : A simple lock variable gives MUTUAL EXCLUSION —
               only one consumer may claim an alert at a time.

   NOTE: browser JavaScript is single-threaded, so Worker 1 and
   Worker 2 are SIMULATED workers (a UI demonstration of the
   OS concept), not real operating-system threads.
   ============================================================ */

/* ---------- Settings ---------- */
const MAX_BUFFER = 5;        // bounded buffer capacity
const PROCESS_TIME = 900;    // simulated response time (milliseconds)

// Details for each sensor (producer)
const SENSOR_INFO = {
  smoke: { name: "Smoke Sensor", icon: "💨", message: "Smoke detected" },
  heat:  { name: "Heat Sensor",  icon: "🌡️", message: "Temperature high" },
  flame: { name: "Flame Sensor", icon: "🔥", message: "Flame detected" }
};

/* ---------- Shared state ---------- */
let alertBuffer = [];      // the shared bounded buffer (FIFO queue)
let processedLog = [];     // alerts already handled (newest first)
let counters = { total: 0, smoke: 0, heat: 0, flame: 0 };
let nextAlertId = 1;       // gives every alert a unique ID
let isProcessing = false;  // the mutual-exclusion lock (true = a consumer is busy)
let lockOwner = null;      // name of the worker holding the lock
let lastWorker = null;     // used to alternate between the two workers
let processTimer = null;   // timer for the simulated processing time

/* ---------- Small helper ---------- */
// Current clock time, e.g. "10:15:32 AM"
function currentTime() {
  return new Date().toLocaleTimeString();
}

// Show a message in the message bar (success / error / info / warn)
function showMessage(text, type) {
  const bar = document.getElementById("messageBar");
  bar.textContent = text;
  bar.className = "message-bar " + type;
}

// Alternate between Worker 1 and Worker 2
function nextWorkerName() {
  lastWorker = (lastWorker === "Worker 1") ? "Worker 2" : "Worker 1";
  return lastWorker;
}

/* ============================================================
   PRODUCER — a sensor generates an alert and pushes it
   into the shared buffer (FIFO: enter at the back).
   ============================================================ */
function produceAlert(sensorType, location, quiet) {
  const info = SENSOR_INFO[sensorType];

  // --- Buffer overflow: reject the alert when the queue is full ---
  if (alertBuffer.length >= MAX_BUFFER) {
    showMessage(
      "⛔ BUFFER FULL (" + alertBuffer.length + "/" + MAX_BUFFER + ") — " +
      info.name + " alert for " + location + " was REJECTED (no space in the queue).",
      "error"
    );
    return false;
  }

  // Create a new alert and add it to the BACK of the queue
  const alert = {
    id: nextAlertId,
    sensor: sensorType,
    sensorName: info.name,
    icon: info.icon,
    location: location,
    message: info.message,
    time: currentTime(),
    status: "Pending",
    worker: null
  };

  alertBuffer.push(alert);   // FIFO: producers enter at the back
  nextAlertId++;
  counters.total++;
  counters[sensorType]++;
  renderAll();

  if (!quiet) {
    showMessage(
      "✅ Producer generated ALERT #" + alert.id + " — " +
      info.name + " (" + info.message + ") at " + location + ".",
      "success"
    );
  }
  return true;
}

/* ============================================================
   CONSUMER — a worker claims the OLDEST alert (front of the
   queue) and processes it. The lock (isProcessing) guarantees
   MUTUAL EXCLUSION: no two consumers at the same time.
   ============================================================ */
function processOne(silentWhenEmpty) {
  return new Promise(function (resolve) {

    // --- Mutual exclusion: another consumer already holds the lock ---
    if (isProcessing) {
      showMessage(
        "🔒 MUTUAL EXCLUSION — " + lockOwner + " is already processing ALERT #" +
        alertBuffer[0].id + ". No other consumer can claim an alert until it finishes.",
        "error"
      );
      resolve(false);
      return;
    }

    // --- Empty buffer: nothing to process ---
    if (alertBuffer.length === 0) {
      if (!silentWhenEmpty) {
        showMessage("📭 No alerts waiting — the buffer is empty.", "warn");
      }
      resolve(false);
      return;
    }

    // A worker claims the next alert
    isProcessing = true;
    lockOwner = nextWorkerName();
    const alert = alertBuffer[0];
    alert.status = "Processing";
    alert.worker = lockOwner;
    renderAll();
    showMessage(
      "🧑‍🚒 " + lockOwner + " claimed ALERT #" + alert.id + " (" + alert.location +
      ") — buffer is locked.",
      "info"
    );

    // Simulated response time, then the alert leaves the buffer
    processTimer = setTimeout(function () {
      processTimer = null;
      alertBuffer.shift();          // remove from the FRONT of the queue (FIFO)
      alert.status = "Completed";
      processedLog.unshift(alert);  // add to the processed log
      const worker = lockOwner;
      isProcessing = false;         // release the lock
      lockOwner = null;
      renderAll();
      showMessage(
        "✅ " + worker + " finished ALERT #" + alert.id + " — " +
        alert.location + " (" + alert.message + ").",
        "success"
      );
      resolve(true);
    }, PROCESS_TIME);
  });
}

// Process every waiting alert, one after another
async function processAll() {
  if (isProcessing) {
    showMessage("🔒 " + lockOwner + " is busy — please wait until it finishes.", "error");
    return;
  }
  if (alertBuffer.length === 0) {
    showMessage("📭 No alerts waiting — nothing to process.", "warn");
    return;
  }

  const total = alertBuffer.length;
  showMessage("⏳ Processing " + total + " alert(s) in FIFO order...", "info");

  let count = 0;
  let ok = await processOne(true);   // silent: the loop reports the final result
  while (ok) {
    count++;
    ok = await processOne(true);
  }

  if (count > 0) {
    showMessage(
      "✅ Processed " + count + " alert(s). Buffer is now " +
      (alertBuffer.length === 0 ? "empty." : "size " + alertBuffer.length + "."),
      "success"
    );
  }
}

/* ============================================================
   DEMO BUTTONS
   ============================================================ */

// Insert the three example alerts used in the journal demo
function loadSampleAlerts() {
  const samples = [
    { sensor: "smoke", room: "ROOM 101" },   // Smoke detected
    { sensor: "heat",  room: "ROOM 204" },   // Temperature high
    { sensor: "flame", room: "LAB 3" }       // Flame detected
  ];

  // Check there is enough space first
  const freeSlots = MAX_BUFFER - alertBuffer.length;
  if (freeSlots < samples.length) {
    showMessage(
      "⛔ Not enough space — only " + freeSlots + " free slot(s) but 3 needed. " +
      "Process some alerts or reset the demo first.",
      "error"
    );
    return;
  }

  samples.forEach(function (s) {
    produceAlert(s.sensor, s.room, true); // quiet: we show one summary message
  });

  showMessage(
    "📥 Loaded 3 sample alerts: ROOM 101 → Smoke detected, " +
    "ROOM 204 → Temperature high, LAB 3 → Flame detected.",
    "success"
  );
}

// Reset everything back to the initial state
function resetDemo() {
  if (processTimer !== null) {   // stop any alert currently being processed
    clearTimeout(processTimer);
    processTimer = null;
  }
  alertBuffer = [];
  processedLog = [];
  counters = { total: 0, smoke: 0, heat: 0, flame: 0 };
  nextAlertId = 1;
  isProcessing = false;
  lockOwner = null;
  lastWorker = null;
  renderAll();
  showMessage("↻ Demo reset — buffer empty, log cleared, counters back to zero.", "info");
}

/* ============================================================
   RENDERING — keeps the page in sync with the state
   ============================================================ */
function renderSummary() {
  document.getElementById("totalCount").textContent = counters.total;
  document.getElementById("smokeCount").textContent = counters.smoke;
  document.getElementById("heatCount").textContent = counters.heat;
  document.getElementById("flameCount").textContent = counters.flame;
}

function renderBuffer() {
  const list = document.getElementById("bufferList");

  list.innerHTML = alertBuffer.map(function (a) {
    const cls = (a.status === "Processing") ? "processing" : "pending";
    const who = a.worker ? " — " + a.worker : "";
    return '<div class="alert-row ' + cls + '">' +
             '<span class="a-id">#' + a.id + '</span>' +
             '<span class="a-room">' + a.location + '</span>' +
             '<span class="a-type">' + a.icon + ' ' + a.sensorName + '</span>' +
             '<span class="a-time">' + a.time + '</span>' +
             '<span class="a-status ' + cls + '">' + a.status + who + '</span>' +
           '</div>';
  }).join("");

  // Buffer capacity gauge (5 dots)
  document.getElementById("bufferGauge").innerHTML =
    Array.from({ length: MAX_BUFFER }, function (_, i) {
      return '<span class="dot' + (i < alertBuffer.length ? ' filled' : '') + '"></span>';
    }).join("");

  document.getElementById("queueSize").textContent = alertBuffer.length;
  document.getElementById("bufferEmpty").style.display =
    (alertBuffer.length === 0) ? "block" : "none";
}

function renderLog() {
  const list = document.getElementById("processingLog");

  list.innerHTML = processedLog.map(function (a) {
    return '<div class="log-item">' +
             '✅ ALERT #' + a.id + ' — ' + a.location + ' · ' + a.icon + ' ' +
             a.sensorName + ' · "' + a.message + '"' +
             '<div class="log-meta">Handled by ' + a.worker + ' at ' + a.time +
             ' · finished ' + currentTime() + '</div>' +
           '</div>';
  }).join("");

  document.getElementById("logEmpty").style.display =
    (processedLog.length === 0) ? "block" : "none";
}

function renderWorkers() {
  const busy1 = (lockOwner === "Worker 1");
  const busy2 = (lockOwner === "Worker 2");

  const w1 = document.getElementById("worker1");
  const w2 = document.getElementById("worker2");
  w1.className = "worker" + (busy1 ? " busy" : "");
  w2.className = "worker" + (busy2 ? " busy" : "");
  document.getElementById("worker1Status").textContent = busy1 ? "Busy" : "Idle";
  document.getElementById("worker2Status").textContent = busy2 ? "Busy" : "Idle";

  const lockBox = document.getElementById("lockBox");
  if (lockOwner) {
    lockBox.textContent = "🔒 Buffer locked by " + lockOwner;
    lockBox.className = "lock-box locked";
  } else {
    lockBox.textContent = "🔓 Buffer free — no consumer holds the lock";
    lockBox.className = "lock-box free";
  }
}

function renderFlow() {
  document.getElementById("flowBufferSize").textContent =
    alertBuffer.length + " / " + MAX_BUFFER;

  const badge = document.getElementById("systemBadge");
  if (alertBuffer.length >= MAX_BUFFER) {
    badge.textContent = "⛔ BUFFER FULL";
    badge.className = "system-badge full";
  } else if (isProcessing) {
    badge.textContent = "🚨 RESPONSE ACTIVE";
    badge.className = "system-badge active";
  } else {
    badge.textContent = "SYSTEM ONLINE";
    badge.className = "system-badge online";
  }
}

function renderAll() {
  renderSummary();
  renderBuffer();
  renderLog();
  renderWorkers();
  renderFlow();
}

/* ============================================================
   START THE APP
   ============================================================ */
function initApp() {
  // Wire up the three producer (sensor) buttons
  document.querySelectorAll(".btn-produce").forEach(function (btn) {
    btn.addEventListener("click", function () {
      const sensor = btn.dataset.sensor;
      const select = document.getElementById(sensor + "Location");
      produceAlert(sensor, select.value);
    });
  });

  // Wire up the consumer and demo buttons
  document.getElementById("processNextBtn").addEventListener("click", function () {
    processOne(false);
  });
  document.getElementById("processAllBtn").addEventListener("click", processAll);
  document.getElementById("sampleBtn").addEventListener("click", loadSampleAlerts);
  document.getElementById("resetBtn").addEventListener("click", resetDemo);

  renderAll();
  showMessage("System ready. Trigger a sensor alert or load the sample alerts to begin.", "info");
}

// Load the app when the page is ready (the guard also lets the
// logic be tested outside the browser, e.g. with Node.js)
if (typeof window !== "undefined") {
  window.addEventListener("load", initApp);
}
