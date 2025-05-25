import express from "express";
import { connectFyers, disconnectFyers, testConnection, getHistory } from "../controllers/fyers.controller.mjs";
import {
  initRedisClient,
  getMarketData,
  getAllMarketData,
  getMarket30Data,
  getAllMarket30Data,
  calculateByAmount,
  getDataByAmount,
  getAllHistoryData,
  getDataByVol,
  getVolumeHistoryFromFile,
  saveTradeDataToRedis,
} from "../controllers/redis.controller.mjs";
import { initFyersNew } from "../controllers/fyersnew.controller.mjs";
import { simulateStockUpdates, stopStockUpdates } from "../controllers/publisher.mjs";
import {
  getExchangeInfo,
  testBinanceClient,
  startApp,
  listenBinanceCandles,
  getAccountInfo,
  getCandlesticksInMinute,
  getCandlesticksByAmount,
  getPast5DaysVolume,
  getCandlesticksByVolume,
} from "../controllers/binance.controller.mjs";

const router = express.Router();

// Define routes
router.post("/init", async (req, res) => {
  await initRedisClient();
  connectFyers(req, res);
  res.send({ status: 200, message: "Init successful" });
});

router.post("/newinit", async (req, res) => {
  // await initRedisClient();
  // connectFyers(req, res);
  initFyersNew();
  simulateStockUpdates(req, res);
  res.send({ status: 200, message: "Init successful" });
});

router.get("/stop-dummy-stock-updates", stopStockUpdates);

router.get("/disconnect", disconnectFyers);

router.get("/test", testConnection);

router.get("/redis", async (req, res) => {
  await initRedisClient();
  res.send({ status: 200, message: "Redis initialized" });
});

// /get-market-data?symbol=NSE:RELIANCE-EQ,NSE:TCS-EQ
router.get("/get-market-data", async (req, res) => {
  const { symbol } = req.query;
  const data = await getMarketData(symbol);
  res.send({ status: 200, data });
});

// /get-all-market-data
router.get("/get-all-market-data", async (req, res) => {
  const data = await getAllMarketData();
  res.send({ status: 200, data });
});

// /get-market-30-data?symbol=NSE:RELIANCE-EQ,NSE:TCS-EQ
router.get("/get-market-30-data", async (req, res) => {
  const { symbol } = req.query;
  const data = await getMarket30Data(symbol);
  res.send({ status: 200, data });
});

// /get-all-market-30-data
router.get("/get-all-market-30-data", async (req, res) => {
  const data = await getAllMarket30Data();
  res.send({ status: 200, data });
});

// /calculate-by-amount?amount=1000000
router.get("/calculate-by-amount", async (req, res) => {
  const { amount, days } = req.query;
  calculateByAmount(amount, days);
  res.send({ status: 200, message: "Calculation started" });
});

router.get("/get-by-amount", async (req, res) => {
  const { amount } = req.query;
  const data = await getDataByAmount(amount);
  res.send({ status: 200, data });
});

// /get-by-vol?vol=1
router.get("/get-by-vol", async (req, res) => {
  const { vol, days } = req.query;
  const data = await getDataByVol(vol, days);
  res.send({ status: 200, data });
});

router.get("/get-history", getHistory);

router.get("/get-all-history", async (req, res) => {
  const data = await getAllHistoryData();
  res.send({ status: 200, data });
});

router.get("/get-volume-file-history", async (req, res) => {
  const data = await getVolumeHistoryFromFile();
  if (data.error) {
    res.send({ status: 400, message: "Error getting volume history from file" });
  } else {
    res.send({ status: 200, data });
  }
});

// POST /start
// router.post("/start", (req, res) => {
//   if (shouldBeRunning) {
//     return res.status(400).json({ message: "Process is already running or starting." });
//   }
//   if (!isRedisReady) {
//     // Optionally try reconnecting Redis here, or just report status
//     console.warn("Start requested, but Redis is not connected. WebSocket will not save data initially.");
//     // return res.status(503).json({ message: "Cannot start: Redis is not connected." });
//   }

//   console.log("API: Received request to start WebSocket listener.");
//   shouldBeRunning = true;
//   connectWebSocket(); // Initiate connection

//   res.status(200).json({ message: "WebSocket listener process started." });
// });

router.get("/start-binance", async (req, res) => {
  const binanceClient = await startApp();
  if (binanceClient) {
    res.send({ status: 200, message: "Binance started", binanceClient });
  } else {
    res.send({ status: 400, message: "Error starting binance client" });
  }
});

router.get("/get-binance-exchange-info", async (req, res) => {
  const response = await getExchangeInfo();
  if (response) {
    res.send({ status: 200, message: "Binance exchange info", response });
  } else {
    res.send({ status: 400, message: "Error getting binance exchange info" });
  }
});

router.get("/test-binance", async (req, res) => {
  const response = await testBinanceClient();
  if (response) {
    res.send({ status: 200, message: "Tested", response });
  } else {
    res.send({ status: 400, message: "Error testing binance client" });
  }
});

router.get("/listen-binance-candles", async (req, res) => {
  const response = await listenBinanceCandles();
  if (response) {
    res.send({ status: 200, message: "Binance candles listened", response });
  } else {
    res.send({ status: 400, message: "Error listening binance candles" });
  }
});

router.get("/get-binance-account-info", async (req, res) => {
  const response = await getAccountInfo();
  if (response) {
    res.send({ status: 200, message: "Binance account info", response });
  } else {
    res.send({ status: 400, message: "Error getting binance account info" });
  }
});

router.get("/get-binance-candlesticks", async (req, res) => {
  const response = await getCandlesticksInMinute();
  if (response) {
    res.send({ status: 200, message: "Binance candlesticks", response });
  } else {
    res.send({ status: 400, message: "Error getting binance candlesticks" });
  }
});

// get-binance-candlesticks-by-amount?amount=1000000
router.get("/get-binance-candlesticks-by-amount", async (req, res) => {
  const { amount } = req.query;
  const response = await getCandlesticksByAmount(amount);
  if (response) {
    res.send({ status: 200, message: "Binance candlesticks by amount", response });
  }
});

// get past 5 days volume
router.get("/get-past-5-days-volume", async (req, res) => {
  const response = await getPast5DaysVolume();
  if (response) {
    res.send({ status: 200, message: "Past 5 days volume", response });
  }
});

// get-binance-candlesticks-by-volume?volume=1
router.get("/get-binance-candlesticks-by-volume", async (req, res) => {
  const { volume } = req.query;
  const response = await getCandlesticksByVolume(volume);
  res.send({ status: 200, message: "Binance candlesticks by volume", response });
});

// // POST /stop
// router.post("/stop", (req, res) => {
//   if (!shouldBeRunning) {
//     return res.status(400).json({ message: "Process is already stopped." });
//   }

//   console.log("API: Received request to stop WebSocket listener.");
//   shouldBeRunning = false; // Signal that we shouldn't reconnect

//   if (ws) {
//     console.log("API: Closing active WebSocket connection...");
//     ws.close(1000, "Stopped by API request"); // Graceful close
//     ws = null; // Clear immediately
//     isWsConnected = false;
//     isWsConnecting = false;
//   } else {
//     console.log("API: No active WebSocket connection to close.");
//     isWsConnected = false;
//     isWsConnecting = false;
//   }

//   res.status(200).json({ message: "WebSocket listener process stopped." });
// });

// // GET /status
// router.get("/status", (req, res) => {
//   res.status(200).json({
//     serviceShouldRun: shouldBeRunning,
//     redisConnected: isRedisReady,
//     websocketConnecting: isWsConnecting,
//     websocketConnected: isWsConnected,
//     websocketInstanceExists: !!ws, // More detailed state
//   });
// });

export default router;
