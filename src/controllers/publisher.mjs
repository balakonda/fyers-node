import { Worker } from "worker_threads";
let publisherWorker = null;

export const simulateStockUpdates = (req, res) => {
  if (publisherWorker) {
    return res.json({ message: "Publishing is already running!" });
  }

  publisherWorker = new Worker(new URL("./publisherWorker.mjs", import.meta.url), {
    workerData: {
      accessToken: req.body.accessToken,
    },
  });

  publisherWorker.on("message", (msg) => console.log("Publisher:", msg));
  publisherWorker.on("error", (err) => console.error("Publisher Error:", err));

  res.json({ message: "Publishing started!" });
};

export const stopStockUpdates = (req, res) => {
  if (!publisherWorker) {
    return res.json({ message: "Publishing is not running!" });
  }

  publisherWorker.terminate();
  publisherWorker = null;

  res.json({ message: "Publishing stopped!" });
};
