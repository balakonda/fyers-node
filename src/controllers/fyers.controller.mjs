import { NIFTY_200_LIST } from "../data/data.mjs";
import { setMarketData, setHistoryData, getHistoryData } from "./redis.controller.mjs";
import { FYERS_CLIENT_ID } from "../data/env.mjs";
import { fyersDataSocket, fyersModel } from "fyers-api-v3";
import { STOCK_LIST } from "../data/data.mjs";
import dayjs from "dayjs";
import fs from "fs";

var fyersdata;
var fyersAPI;
const clientId = FYERS_CLIENT_ID;

function getFyersWebSocket(accessToken) {
  if (fyersdata && fyersdata?.isConnected()) {
    // return;
    console.log("fyersdata already connected");
    return;
  }

  fyersdata = new fyersDataSocket(`${clientId}:${accessToken}`);

  function onmsg(message) {
    // console.log("3Received market data:", message);

    if (message.symbol) {
      // console.log("message", message.symbol);
      // {"symbol":"NSE:ACC-EQ","ltp":1986.7,"lower_ckt":0,"upper_ckt":0,"vol_traded_today":0,"last_traded_time":1744193773,"bid_size":0,"ask_size":0,"bid_price":0,"ask_price":0,"last_traded_qty":3,"tot_buy_qty":0,"tot_sell_qty":0,"avg_trade_price":0,"low_price":0,"high_price":0,"open_price":0,"prev_close_price":1986.7,"ch":0,"chp":0,"type":"sf"}

      // Save market data to a file
      // Save data to Redis with TTL (2 hours = 7200 seconds)
      setMarketData(message);

      // fs.appendFile("market-data.txt", JSON.stringify(message) + "\n", (err) => {
      //   if (err) {
      //     console.error("Error saving data:", err);
      //   } else {
      //     console.log("Market data saved to file");
      //   }
      // });
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

export const connectFyers = (req) => {
  const { accessToken } = req.body;
  getFyersWebSocket(accessToken);
  getFyersAPI(accessToken);
};

export const disconnectFyers = (req, res) => {
  fyersdata?.unsubscribe(STOCK_LIST);
  res.send({ status: 200, message: "Fyers disconnected" });
};

export const testConnection = (req, res) => {
  const isConnected = fyersdata?.isConnected();
  console.log("isConnected", isConnected);
  res.send({ status: 200, message: isConnected });
};

export const getFyersAPI = (accessToken) => {
  if (!fyersAPI) {
    fyersAPI = new fyersModel();
    fyersAPI.setAppId(clientId);
    fyersAPI.setAccessToken(accessToken);
  }
  return fyersAPI;
};

const DateFormat = "0";

export const getHistory = async (req, res) => {
  if (!fyersAPI) return res.send({ status: 400, message: "Fyers API not connected" });
  try {
    const historyData = await getHistoryData(STOCK_LIST[0]);
    // console.log("historyData", historyData);
    if (historyData) {
      return res.send({ status: 200, message: "Already fetched" });
    }

    const volList = {};

    // Process symbols one at a time with a 1-second delay to avoid hitting API rate limits
    for (const symbol of STOCK_LIST) {
      try {
        // History of past 10 days
        const range_from = dayjs().subtract(20, "day").unix(); // epoch time in seconds
        const range_to = dayjs().subtract(1, "day").unix(); // epoch time in seconds
        var inp = {
          symbol: symbol,
          resolution: "D",
          date_format: DateFormat,
          range_from: range_from,
          range_to: range_to,
          cont_flag: "1",
        };
        const history = await fyersAPI.getHistory(inp);
        // history.candles is an array of objects with date and close price
        // 1.Current epoch time
        // 2. Open Value
        // 3.Highest Value
        // 4.Lowest Value
        // 5.Close Value
        // 6.Total traded quantity (volume)
        const newlist = [];
        let totalVolume = 0;
        let totalDays = 0;
        const length = history.candles.length;
        const maxDays = 10;
        let volByDays = {
          1: {
            volume: 0,
            days: 0,
          },
          2: {
            volume: 0,
            days: 0,
          },
          3: {
            volume: 0,
            days: 0,
          },
          4: {
            volume: 0,
            days: 0,
          },
          5: {
            volume: 0,
            days: 0,
          },
          6: {
            volume: 0,
            days: 0,
          },
          7: {
            volume: 0,
            days: 0,
          },
          8: {
            volume: 0,
            days: 0,
          },
          9: {
            volume: 0,
            days: 0,
          },
          10: {
            volume: 0,
            days: 0,
          },
        };
        // Loop from last for max 10 days
        for (let i = length - 1; i >= 0 && i >= length - maxDays; i--) {
          totalVolume += history.candles[i][5];
          totalDays++;
          volByDays[totalDays].volume += totalVolume;
          volByDays[totalDays].days = totalDays;
        }

        const formattedVolByDays = {};

        for (const [key, value] of Object.entries(volByDays)) {
          formattedVolByDays[key] = {
            volume: value.volume,
            days: value.days,
            avgVolume: Math.round(value.volume / (value.days * 375)),
          };
        }
        const payload = {
          totalVolume,
          totalDays,
          avgVolume: Math.round(totalVolume / (totalDays * 375)), // 375 is the number of trading minutes in a day
          // history: history.candles,
          volByDays: formattedVolByDays,
        };
        await setHistoryData(symbol, payload);

        volList[symbol] = payload;

        // Add a 1-second delay between API calls to avoid hitting rate limits
        await new Promise((resolve) => setTimeout(resolve, 200));
        // console.log(`Fetched history for ${symbol}`);
      } catch (error) {
        console.error(`Error fetching history for ${symbol}:`, error);
      }
    }

    const currentDay = new Date().getDate();
    fs.writeFileSync(`vol-list-${currentDay}.json`, JSON.stringify(volList));

    res.send({ status: 200, message: "History fetched" });
  } catch (err) {
    console.log(err);
    res.send({ status: 400, message: err });
  }
};
