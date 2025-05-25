import { Worker } from "worker_threads";
import { STOCK_LIST } from "../data/data.mjs";

const NUM_WORKERS = 5;
const STOCKS = STOCK_LIST;

const chunkSize = Math.ceil(STOCKS.length / NUM_WORKERS);
const workers = [];

export const initFyersNew = () => {
  console.log("Initializing Fyers New");
  if (workers.length > 0) {
    console.log("Workers already initialized");
    return;
  }
  for (let i = 0; i < NUM_WORKERS; i++) {
    const workerStocks = STOCKS.slice(i * chunkSize, (i + 1) * chunkSize);
    const worker = new Worker(new URL("./worker.mjs", import.meta.url), {
      workerData: { stocks: workerStocks, workerId: i },
    });

    worker.on("message", (msg) => console.log(`Worker ${msg.workerId}:`, msg));
    worker.on("error", (err) => console.error("Worker Error:", err));

    workers.push(worker);
  }
};
