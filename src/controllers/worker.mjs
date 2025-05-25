import { workerData, parentPort } from "worker_threads";
import { createClient } from "redis";

const redisClient = createClient({
  url: "redis://localhost:6379/0",
  // legacyMode: true,
});

await redisClient.connect();
await redisClient.subscribe("stockUpdates", (message) => {
  //   console.log("Received message:", message);
  const stockData = JSON.parse(message);
  if (workerData.stocks.includes(stockData.symbol)) {
    // console.log("Sending message to parent:", { workerId: workerData.workerId, data: stockData });
    parentPort.postMessage({ workerId: workerData.workerId, data: stockData });
  }
});

console.log(`Worker ${workerData.workerId} handling ${workerData.stocks.length} stocks`);

process.on("exit", async () => {
  await redisClient.quit();
});
