import { workerData, parentPort } from "worker_threads";
import { FYERS_CLIENT_ID } from "../data/env.mjs";
import { fyersDataSocket, fyersModel } from "fyers-api-v3";
import { createClient } from "redis";
import { STOCK_LIST } from "../data/data.mjs";
export const ExpiryTime = { EX: 86400 };
const redisClient = createClient({
  url: "redis://localhost:6379/0",
});
await redisClient.connect();

// Function to publish stock updates
async function publishStockUpdate(message) {
  // Get Current minute start time epoch
  const currentTime = new Date();
  const currentMinuteStartTime = new Date(currentTime.getFullYear(), currentTime.getMonth(), currentTime.getDate(), currentTime.getHours(), currentTime.getMinutes(), 0, 0);
  const currentMinuteEpoch = currentMinuteStartTime.getTime();
  const data = JSON.stringify({
    s: message.symbol,
    ltp: message.ltp,
    v: message.vol_traded_today,
  });
  await redisClient.publish("stockUpdates", data);
  await redisClient.hSet("stocks", `${currentMinuteEpoch}:${message.symbol}`, message.ltp * message.vol_traded_today, {
    EX: ExpiryTime,
  });
}

parentPort.postMessage({ message: "Publishing started from worker" });

// Set up Fyers API
var fyersdata;
var fyersAPI;
const clientId = FYERS_CLIENT_ID;

function getFyersWebSocket(accessToken) {
  if (fyersdata && fyersdata?.isConnected()) {
    // return;
    console.log("fyersdata already connected");
    return;
  }
  console.log("Creating new fyersdata", clientId, accessToken);
  fyersdata = new fyersDataSocket(`${clientId}:${accessToken}`);

  function onmsg(message) {
    // console.log("3Received market data:", message);

    if (message.symbol) {
      // console.log("message", message.symbol);
      // {"symbol":"NSE:ACC-EQ","ltp":1986.7,"lower_ckt":0,"upper_ckt":0,"vol_traded_today":0,"last_traded_time":1744193773,"bid_size":0,"ask_size":0,"bid_price":0,"ask_price":0,"last_traded_qty":3,"tot_buy_qty":0,"tot_sell_qty":0,"avg_trade_price":0,"low_price":0,"high_price":0,"open_price":0,"prev_close_price":1986.7,"ch":0,"chp":0,"type":"sf"}
      // Save market data to a file
      // Save data to Redis with TTL (2 hours = 7200 seconds)
      //   setMarketData(message);
      // fs.appendFile("market-data.txt", JSON.stringify(message) + "\n", (err) => {
      //   if (err) {
      //     console.error("Error saving data:", err);
      //   } else {
      //     console.log("Market data saved to file");
      //   }
      // });
      publishStockUpdate(message);
    }
  }

  function onconnect() {
    fyersdata.subscribe(STOCK_LIST); //not subscribing for market depth data
    // fyersdata.mode(fyersdata.LiteMode) //set data mode to lite mode
    // fyersdata.mode(fyersdata.FullMode) //set data mode to full mode is on full mode by default
    fyersdata.autoreconnect(6); //enable auto reconnection mechanism in case of disconnection
  }

  function onerror(err) {
    console.log(err);
  }

  function onclose() {
    console.log("socket closed");
  }

  // function ontrades(data) {
  //   console.log("Received trades:", data);
  // }

  // fyersdata.on("trades", ontrades);
  fyersdata.on("message", onmsg);
  fyersdata.on("connect", onconnect);
  fyersdata.on("error", onerror);
  fyersdata.on("close", onclose);

  // Initialize WebSocket connection
  fyersdata.connect(STOCK_LIST);

  console.log("Fyers WebSocket server is listening...");
}

export const getFyersAPI = (accessToken) => {
  if (!fyersAPI) {
    fyersAPI = new fyersModel();
    fyersAPI.setAppId(clientId);
    fyersAPI.setAccessToken(accessToken);
  }
  return fyersAPI;
};

export const connectFyers = () => {
  console.log("Connecting to Fyers", workerData.accessToken);
  getFyersWebSocket(workerData.accessToken);
  getFyersAPI(workerData.accessToken);
};
connectFyers();
// Fyers API Stop

process.on("exit", async () => {
  // clearInterval(interval);
  await redisClient.quit();
});
