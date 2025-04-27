import express from "express";
import { connectFyers, disconnectFyers, testConnection, getHistory } from "../controllers/fyers.controller.mjs";
import { initRedisClient, getMarketData, getAllMarketData, getMarket30Data, getAllMarket30Data, calculateByAmount, getDataByAmount } from "../controllers/redis.controller.mjs";

const router = express.Router();

// Define routes
router.post("/init", async (req, res) => {
  await initRedisClient();
  connectFyers(req, res);
  res.send({ status: 200, message: "Init successful" });
});

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
  const { amount } = req.query;
  calculateByAmount(amount);
  res.send({ status: 200, message: "Calculation started" });
});

router.get("/get-by-amount", async (req, res) => {
  const { amount } = req.query;
  const data = await getDataByAmount(amount);
  res.send({ status: 200, data });
});

router.get("/get-history", getHistory);

export default router;
