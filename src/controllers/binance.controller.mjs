import Binance from "binance-api-node";

import WebSocket from "ws";

// --- Configuration ---
// --- Configuration ---
const symbol = "btcusdt"; // Change to the trading pair you want (lowercase)
const streamName = `${symbol}@trade`; // Stream type: @trade, @kline_1m, @depth, etc.
const websocketUrl = `wss://stream.binance.com:9443/ws/${streamName}`;
const redisStreamMaxLen = 1000000; // Approx. max number of entries in the stream (adjust as needed!) Use '~' for approximate trimming.
const reconnectInterval = 5000; // Milliseconds to wait before attempting WebSocket reconnection

// --- WebSocket Connection ---
let ws;
let binanceClient;

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

async function listenBinanceCandles() {
  if (!binanceClient) {
    console.error("Binance client not initialized");
    return;
  }
  binanceClient.ws.candles(["ETHBTC", "BNBBTC"], { interval: "1m" }, (data) => {
    console.log("listenBinanceCandles", data);
  });
  binanceClient.ws.candles("BTCUSDT", "1m", (candlestick) => {
    const { e: eventType, E: eventTime, s: symbol, k: ticks } = candlestick;
    console.log("candlestick", candlestick);
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
export { startApp, listenToWebSocket, testBinanceClient, getExchangeInfo, listenBinanceCandles, getAccountInfo };
