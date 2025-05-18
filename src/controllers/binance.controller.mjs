import Binance from "binance-api-node";
import { initBinanceRedisClient, getBinanceRedisClient, ExpiryTime } from "./redis.controller.mjs";

// --- WebSocket Connection ---
let binanceClient;
const sortedSetKey = "candlestick_index";

// function connectWebSocket() {

//   console.log(`Attempting to connect to WebSocket: ${websocketUrl}`);
//   ws = new WebSocket(websocketUrl);

//   ws.on("open", function open() {
//     console.log("WebSocket connection established.");
//   });

//   ws.on("message", function incoming(data) {
//     const receivedTime = Date.now();
//     if (!isRedisReady) {
//       console.warn("Redis not ready, skipping message saving.");
//       return; // Don't process if Redis isn't connected
//     }

//     try {
//       const messageString = data.toString("utf8");
//       const tradeData = JSON.parse(messageString);

//       if (tradeData.e === "trade" && tradeData.t) {
//         saveTradeDataToRedis(tradeData, receivedTime);
//       } else {
//         console.log("Received non-trade message or unexpected format:", tradeData);
//       }
//     } catch (error) {
//       console.error("Error parsing JSON or processing message:", error);
//       console.error("Received data:", data.toString("utf8"));
//     }
//   });

//   ws.on("ping", function ping(data) {
//     console.log("Received ping, sending pong.");
//     ws.pong(data);
//   });

//   ws.on("pong", function pong() {
//     console.log("Received pong.");
//   });

//   ws.on("error", function error(err) {
//     console.error("WebSocket error:", err.message);
//   });

//   ws.on("close", function close(code, reason) {
//     console.log(`WebSocket connection closed. Code: ${code}, Reason: ${reason ? reason.toString() : "N/A"}`);
//     console.log(`Attempting to reconnect WebSocket in ${reconnectInterval / 1000} seconds...`);
//     ws = null;
//     setTimeout(connectWebSocket, reconnectInterval);
//   });
// }

// async function checkWebSocketConnection() {
//   if (!ws) {
//     console.log("WebSocket connection not established.");
//     return false;
//   }
//   return true;
// }

// // --- Data Saving to Redis Stream ---
// async function saveTradeDataToRedis(trade, receivedTime) {
//   try {
//     // Use XADD to add the trade to the Redis Stream
//     // '*' tells Redis to auto-generate a unique ID (timestamp-based)
//     // MAXLEN ~ redisStreamMaxLen caps the stream size (approximate)
//     const streamData = {
//       // Flatten the object for stream fields
//       symbol: trade.s,
//       tradeId: String(trade.t), // Store IDs as strings
//       price: trade.p,
//       quantity: trade.q,
//       buyerOrderId: String(trade.b),
//       sellerOrderId: String(trade.a),
//       tradeTime: String(trade.T),
//       isBuyerMaker: trade.m ? "1" : "0", // Store boolean as '1' or '0'
//       // isBestMatch: trade.M ? '1' : '0', // Often deprecated/not useful, check docs
//       receivedTime: String(receivedTime),
//     };

//     // Add entry to the stream, using tradeId 't' as the message ID for potential deduplication
//     // If you prefer Redis-generated IDs ensuring insertion order: use '*' instead of String(trade.t)
//     // Using '*' might be safer if trade IDs aren't guaranteed monotonic by the source
//     const entryId = await redisClient.xAdd(
//       redisStreamKey,
//       "*", // Let Redis generate the entry ID
//       streamData,
//       {
//         // XADD options
//         TRIM: {
//           strategy: "MAXLEN", // Trim by length
//           strategyModifier: "~", // Allow approximate trimming for performance
//           threshold: redisStreamMaxLen,
//         },
//       }
//     );

//     // Optional: Log the generated stream entry ID
//     // console.log(`Saved trade ${trade.t} to Redis Stream with ID: ${entryId}`);
//   } catch (error) {
//     console.error("Error saving data to Redis:", error);
//     console.error("Failed data:", trade);
//     // Consider how to handle Redis errors (e.g., retry logic, circuit breaker)
//     isRedisReady = false; // Assume connection might be lost on error
//   }
// }

// --- Application Start ---

async function startApp() {
  console.log("Starting binance client");
  try {
    await initBinanceRedisClient();
    if (!binanceClient) {
      // Import Binance from 'binance-api-node' is needed at the top of the file
      binanceClient = Binance.default({
        apiKey: process.env.BINANCE_API_KEY,
        apiSecret: process.env.BINANCE_API_SECRET,
      });
    }
    console.log("Binance client initialized");
    return binanceClient;
  } catch (error) {
    console.error("Error starting binance client:", error);
    return null;
  }
}

// Listen to the WebSocket connection
async function listenToWebSocket() {
  if (!binanceClient) {
    console.error("Binance client not initialized");
    return;
  }
  binanceClient.ws.on("message", function incoming(data) {
    console.log(data);
  });
}

// Store candlestick data (example)
async function storeCandlestick(candlestick) {
  // const key = `candlestick:${data.startTime}`; // Unique key per candlestick
  // await client.hSet(key, data);
  // await client.zAdd("candlestick_index", [{ score: data.startTime, value: key }]); // Indexing by timestamp

  // candlestick = {
  //   eventType: 'kline',
  //   eventTime: 1747377750028,
  //   symbol: 'ETHUSDT',
  //   startTime: 1747377720000,
  //   closeTime: 1747377779999,
  //   firstTradeId: 2426578688,
  //   lastTradeId: 2426579386,
  //   open: '2596.21000000',
  //   high: '2596.26000000',
  //   low: '2595.09000000',
  //   close: '2596.25000000',
  //   volume: '70.62320000',
  //   trades: 699,
  //   interval: '1m',
  //   isFinal: false,
  //   quoteVolume: '183318.09806100',
  //   buyVolume: '50.52680000',
  //   quoteBuyVolume: '131150.64603300'
  // }
  // - eventType: Indicates the type of event. "kline" means this data represents a candlestick (K-line).
  // - eventTime: The timestamp when the event was generated, in milliseconds.
  // - symbol: The trading pair, in this case, Ethereum to USDT (ETHUSDT).
  // - startTime: The timestamp marking the beginning of the candlestick interval.
  // - closeTime: The timestamp marking the end of the candlestick interval.
  // - firstTradeId: The trade ID of the first transaction in this candlestick interval.
  // - lastTradeId: The trade ID of the last transaction in this candlestick interval.
  // - open: The opening price of the candlestick interval.
  // - high: The highest price recorded during the candlestick interval.
  // - low: The lowest price recorded during the candlestick interval.
  // - close: The closing price of the candlestick interval.
  // - volume: The total volume of the asset traded within the candlestick interval.
  // - trades: The number of trades that occurred during the candlestick interval.
  // - interval: The duration of the candlestick, in this case, "1m" for one minute.
  // - isFinal: Boolean value indicating if this candlestick is the final one for the interval (false means it may still be updating).
  // - quoteVolume: The total quoted asset volume traded during the candlestick interval.
  // - buyVolume: The total volume of the asset bought during the candlestick interval.
  // - quoteBuyVolume: The total quoted asset volume bought during the candlestick interval.

  if (!getBinanceRedisClient()) {
    console.log("Binance Redis not connected");
  }
  if (candlestick && getBinanceRedisClient()) {
    const key = `binance:${candlestick.symbol}:${candlestick.startTime}`; // Unique key per candlestick

    // // Create a flattened array of key-value pairs
    // const flattenedObject = Object.entries(candleData).flat();
    // console.log("flattenedObject", flattenedObject);
    // Convert candleData object to field-value pairs for hSet

    try {
      const result = await getBinanceRedisClient().set(key, JSON.stringify(candlestick), ExpiryTime);
      console.log(`Stored candlestick data with ${result} fields for key: ${key}`);

      // Verify data was stored correctly
      // const storedData = await getBinanceRedisClient().hGetAll(key);
      // if (!storedData || Object.keys(storedData).length <= 1) {
      //   console.error(`Only eventType or partial data stored in Redis for key: ${key}`);
      //   console.debug("Attempted to store:", candleData);
      //   console.debug("Actually stored:", storedData); // Getting storedData as [Object: null prototype] { eventType: 'kline' }
      // }
    } catch (error) {
      console.error(`Failed to store candlestick data in Redis: ${error.message}`);
    }
    await getBinanceRedisClient().zAdd(sortedSetKey, { score: candlestick.startTime, value: key });
    // getRedisClient().set(`binance:candles:${candlestick.symbol}:${candlestick.eventTime}`, JSON.stringify(candlestick), ExpiryTime);
  }
}

async function listenBinanceCandles() {
  if (!binanceClient) {
    console.error("Binance client not initialized");
    return;
  }
  // binanceClient.ws.candles(["ETHBTC", "BNBBTC"], { interval: "1m" }, (data) => {
  //   console.log("listenBinanceCandles", data);
  // });
  binanceClient.ws.candles(["BTCUSDT", "ETHUSDT"], "1m", async (candlestick) => {
    console.log("candlestick", candlestick);
    storeCandlestick(candlestick);
  });

  return true;
}

async function testBinanceClient() {
  try {
    const response = await binanceClient.ping();
    console.log(response);
    return response;
  } catch (error) {
    console.error("Error testing binance client:", error);
    return null;
  }
}

// exchangeInfo
async function getExchangeInfo() {
  try {
    const response = await binanceClient.exchangeInfo();
    console.log(response);
    return response;
  } catch (error) {
    console.error("Error getting exchange info:", error);
    return null;
  }
}

// account info
async function getAccountInfo() {
  try {
    const response = await binanceClient.accountInfo();
    console.log(response);
    return response;
  } catch (error) {
    console.error("Error getting account info:", error);
    return null;
  }
}

// Retrieve all candlesticks within a minute
async function getCandlesticksInMinute() {
  const now = Date.now(); // Current timestamp in milliseconds
  const fiveMinutesAgo = now - 5 * 60 * 1000; // Timestamp 5 minutes ago

  try {
    // Use ZRANGEBYSCORE to get members and scores within the time range
    // The range is inclusive by default [min, max]
    const results = await getBinanceRedisClient().zRangeByScore(sortedSetKey, fiveMinutesAgo, now);

    console.log(`Elements added in the last 5 minutes for key "${sortedSetKey}":`, results);

    // The 'results' array will be [member1, score1, member2, score2, ...]
    // We need to iterate through it to extract members and scores
    // const retrievedData = [];
    // for (let i = 0; i < results.length; i++) {
    //   const member = results[i];
    //   const score = parseFloat(results[i + 1]); // Scores are returned as strings, parse them to numbers
    //   retrievedData.push({ member, score });
    //   console.log(`  Member: ${member}, Timestamp (Score): ${score}`);
    // }
    try {
      const candlesticks = await Promise.all(
        results.map(async (key) => {
          const data = await getBinanceRedisClient().get(key);
          return JSON.parse(data);
        })
      );
      return candlesticks;
    } catch (error) {
      console.error("Error retrieving data from Redis:", error);
      throw error;
    }
  } catch (error) {
    console.error("Error retrieving data from Redis:", error);
    throw error;
  } finally {
    // Close the connection if necessary, depending on your application's architecture
    // redis.quit();
  }
  // const keys = await getBinanceRedisClient().zRangeWithScores("candlestick_index", 0, -1);
  // console.log("keys", keys); // [ { value: 'binance:BTCUSDT:1747380300000', score: 1747380300000 }]

  // const candlesticks = await Promise.all(keys.map((key) => getBinanceRedisClient().get(key.value)));

  // // const candlesticks = await Promise.all(keys.map((key) => getBinanceRedisClient().get(key)));
  // return candlesticks;
}

// startApp();

// // --- Graceful Shutdown ---
// process.on("SIGINT", async () => {
//   console.log("\nCaught interrupt signal (Ctrl+C). Shutting down...");

//   if (ws) {
//     console.log("Closing WebSocket connection...");
//     // Prevent reconnection attempts during shutdown
//     clearTimeout(connectWebSocket); // Ensure no pending timeouts trigger reconnection
//     ws.removeAllListeners(); // Remove listeners to prevent errors on close
//     ws.terminate();
//   }

//   if (redisClient && redisClient.isOpen) {
//     // Check if client exists and is connected/connecting
//     console.log("Quitting Redis client...");
//     try {
//       await redisClient.quit(); // Gracefully disconnect
//       console.log("Redis client quit successfully.");
//     } catch (err) {
//       console.error("Error quitting Redis client:", err);
//     }
//   }
//   console.log("Exiting.");
//   process.exit(0);
// });
export { startApp, listenToWebSocket, testBinanceClient, getExchangeInfo, listenBinanceCandles, getAccountInfo, getCandlesticksInMinute };
