import { createClient } from "redis";
import { STOCK_LIST, MINUTES, TRADING_HOURS } from "../data/data.mjs";
import fs from "fs";

let client;

export const initRedisClient = async () => {
  //   const client = await createClient().on("error", (err) => console.log("Redis Client Error", err));

  //   const connected = await client.connect();
  if (client) return;
  client = createClient({
    url: "redis://localhost:6379",
    // legacyMode: true,
  });

  await client.connect();

  await client.ping();

  console.log("Redis connect();", client);

  try {
    // Store some data
    await client.set("myKey", "myValue");

    // Retrieve the data
    const value = await client.get("myKey");
    console.log("Value:", value); // Output: myValue
  } catch (error) {
    console.error("Error:", error);
  }
};

const ExpiryTime = { EX: 86400 }; // 24 hours

export const setMarketData = async (data) => {
  //   console.log("setMarketData", data);
  if (!client || !data) {
    console.error("Redis Client not initialized");
    return;
  }
  try {
    // {"symbol":"NSE:ADANIGREEN-EQ","ltp":955.9,"exch_feed_time":1745228306,"lower_ckt":0,"upper_ckt":0,"type":"sf","vol_traded_today":1992334,"last_traded_time":1745228306,"bid_size":962,"ask_size":168,"bid_price":955.8,"ask_price":955.9,"last_traded_qty":32,"tot_buy_qty":167825,"tot_sell_qty":337536,"avg_trade_price":957.36,"low_price":944,"high_price":966.6,"open_price":954.7,"prev_close_price":947.15,"ch":8.75,"chp":0.92}
    const redisKey = `market-data:${data.symbol}:${Date.now()}`;
    // console.log("setMarketData", typeof JSON);
    const savedData = JSON.stringify({
      symbol: data.symbol,
      ltp: data.ltp,
      type: data.type,
      vol_traded_today: data.vol_traded_today,
      last_traded_time: data.last_traded_time,
      last_traded_qty: data.last_traded_qty,
      tot_buy_qty: data.tot_buy_qty,
      tot_sell_qty: data.tot_sell_qty,
    });
    await client.set(redisKey, savedData, ExpiryTime); // Set expiry to 24 hours

    // last_traded_time
    const lastTradedTime = data.last_traded_time;
    //  Convert to date and get the seconds
    const lastTradedTimeDate = new Date(lastTradedTime * 1000);
    const lastTradedTimeSeconds = lastTradedTimeDate.getSeconds();
    const lastTradedTimeMinutes = lastTradedTimeDate.getMinutes();
    const lastTradedTimeHours = lastTradedTimeDate.getHours();
    const lastTradedTimeDay = lastTradedTimeDate.getDate();
    if (lastTradedTimeSeconds > 0 && lastTradedTimeSeconds < 31) {
      // 30 seconds
      // `market-30:${data.symbol}:DD-HH-MM-SS`
      const redisKey30 = `${data.symbol}:${lastTradedTimeDay}-${lastTradedTimeHours}-${lastTradedTimeMinutes}-30`;
      // Clear all the data for this key
      await client.del(redisKey30);
      await client.set(redisKey30, savedData, ExpiryTime);
    } else if (lastTradedTimeSeconds > 30 && lastTradedTimeSeconds < 60) {
      // 60 seconds
      // `market-60:${data.symbol}:DD-HH-MM-SS`
      const redisKey60 = `${data.symbol}:${lastTradedTimeDay}-${lastTradedTimeHours}-${lastTradedTimeMinutes}-60`;
      // Clear all the data for this key
      await client.del(redisKey60);
      await client.set(redisKey60, savedData, ExpiryTime);
    }

    //
  } catch (error) {
    console.log("data:", data);
    console.error("Error setting market data:", error);
  }
};

export const getMarketData = async (symbol) => {
  if (!client) {
    console.error("Redis Client not initialized");
    return;
  }
  const splitted = symbol?.split(",");
  // Get all market data for a symbol
  let data = {};

  if (splitted && splitted.length > 0) {
    // Use a for...of loop to properly handle async operations
    for (const sym of splitted) {
      try {
        const pattern = `market-data:${sym}:*`;
        console.log("pattern", pattern);

        // Use scan with proper cursor format
        let cursor = 0;
        let keys = [];

        do {
          const result = await client.scan(cursor, {
            MATCH: pattern,
            COUNT: 100,
          });

          cursor = result.cursor;
          keys = keys.concat(result.keys);
        } while (cursor !== 0);

        // Get data for each key
        const symData = [];
        for (const key of keys) {
          const value = await client.get(key);
          if (value) {
            symData.push(value);
          }
        }

        data[sym] = symData;
      } catch (error) {
        console.error("Error getting market data:", error);
        data[sym] = [];
      }
    }
  }

  return data;
};

export const getAllMarketData = async () => {
  try {
    const allStringData = {};
    let cursor = "0";

    do {
      const reply = await client.scan(cursor, { count: 100 });
      console.log(reply.cursor);
      cursor = reply.cursor;
      const keys = reply.keys;

      for (const key of keys) {
        const value = await client.get(key);
        allStringData[key] = value;
      }
    } while (cursor !== 0);

    // console.log("All String Data:", allStringData);
    return allStringData;
  } catch (error) {
    console.error("Error getting all string data:", error);
  } finally {
    // await client.quit();
  }
};

export const getAllMarket30Data = async () => {
  if (!client) {
    console.error("Redis Client not initialized");
    return;
  }
  const list = {};
  const getCurrentTime = new Date();
  const currentHours = getCurrentTime.getHours();
  const currentMinutes = getCurrentTime.getMinutes();
  const currentMinutesMinusOne = currentMinutes - 1;
  const currentMinutesMinusTwo = currentMinutes - 2;
  const currentDay = getCurrentTime.getDate();
  // console.log("currentHours", currentHours);
  // console.log("currentMinutes", currentMinutes);
  // console.log("currentMinutesMinusOne", currentMinutesMinusOne);
  // console.log("currentDay", currentDay);
  if (!client) {
    console.error("Redis Client not initialized");
    return;
  }
  for (const sym of STOCK_LIST) {
    try {
      // Use Scan Pattern to get the data
      const current30RedisKey = `${sym}:${currentDay}-${currentHours}-${currentMinutesMinusOne}-30`;
      const current60RedisKey = `${sym}:${currentDay}-${currentHours}-${currentMinutesMinusOne}-60`;
      // console.log("current30RedisKey", current30RedisKey);
      const current30Data = await client.get(current30RedisKey);
      const current60Data = await client.get(current60RedisKey);
      // console.log("currentData", currentData);
      const previous30RedisKey = `${sym}:${currentDay}-${currentHours}-${currentMinutesMinusTwo}-30`;
      const previous60RedisKey = `${sym}:${currentDay}-${currentHours}-${currentMinutesMinusTwo}-60`;
      // console.log("previousRedisKey", previousRedisKey);
      const previous30Data = await client.get(previous30RedisKey);
      const previous60Data = await client.get(previous60RedisKey);
      // console.log("previousData", previousData);
      list[sym] = {
        current30Data: current30Data ? JSON.parse(current30Data) : null,
        previous30Data: previous30Data ? JSON.parse(previous30Data) : null,
        current60Data: current60Data ? JSON.parse(current60Data) : null,
        previous60Data: previous60Data ? JSON.parse(previous60Data) : null,
      };
    } catch (error) {
      console.error("Error getting market 30 data:", error);
    }
  }
  return list;
};

export const getMarket30Data = async (symbol) => {
  if (!client) {
    console.error("Redis Client not initialized");
    return;
  }
  const splitted = symbol?.split(",");
  const list = [];
  for (const sym of splitted) {
    const getCurrentTime = new Date();
    const currentDay = getCurrentTime.getDate();
    const currentRedisKey = `${sym}:${currentDay}-*`;
    const currentData = await client.get(currentRedisKey);
    list.push({ currentData });
  }
  return list;
};

const amountKey = "amount";
const volKey = "vol";
const baseAmount = 10000000;
let isRunning = false;

export const calculateByAmount = async (amount, days) => {
  if (!client) {
    console.error("Redis Client not initialized");
    return;
  }
  if (isRunning) {
    console.log("calculateByAmount is already running");
    return;
  }
  isRunning = true;

  try {
    let numberOfDays = Number(days) || 10;
    console.log("calculateByAmount", amount, numberOfDays);
    // Loop through the list of symbols
    const list = {};
    const volList = {
      5: {},
      10: {},
      15: {},
      20: {},
    };
    for (let i = 0; i < 10; i++) {
      list[`${baseAmount * i}`] = {};
    }

    // Trading starts at 9:15 AM and Ends at 3:30 PM
    const getCurrentTime = new Date();
    const currentDay = getCurrentTime.getDate();
    const currentHour = getCurrentTime.getHours();
    const currentMinute = getCurrentTime.getMinutes();

    let volHistory = fs.readFileSync(`vol-list-${currentDay}.json`, "utf8");
    // History of volume list from 1 day to 10 days
    // {[Symbol]: { volByDays: {
    //   [day]: {
    //     volume: number,
    //     days: number,
    //     avgVolume: number,
    //   }
    // }
    // }
    // }

    // console.log("volHistory", volHistory);
    if (volHistory) volHistory = JSON.parse(volHistory);

    for (const hour of TRADING_HOURS) {
      if (hour <= currentHour) {
        // Check if the currentRedisKey exists for that hour
        const checkRedisKey = `${amountKey}:${amount}:*:${currentDay}-${hour}-*`;
        console.log("checkRedisKey", checkRedisKey);
        // Scan it using pattern
        let cursor = 0;
        let keys = [];
        // if (hour !== currentHour) {
        //   do {
        //     const result = await client.scan(cursor, {
        //       MATCH: checkRedisKey,
        //       COUNT: 100,
        //     });
        //     cursor = result.cursor;
        //     keys = keys.concat(result.keys);
        //   } while (cursor !== 0);

        //   console.log("keys", keys.length);

        //   if (keys.length > 0) {
        //     // Skip the operation for that hour
        //     console.log("skipping the operation for that hour:", hour);
        //     continue;
        //   }
        //   console.log("calculating for hour", hour);
        // }
        for (const minute of MINUTES) {
          if (hour === currentHour && minute >= currentMinute) {
            // console.log("skipping the operation for that minute:", minute);
            continue;
          }
          try {
            for (const sym of STOCK_LIST) {
              if (!list[sym]) {
                list[sym] = {};
              }
              // Get Current Day
              let currentRedisKey;
              let previousRedisKey;
              if (minute > 0) {
                currentRedisKey = `${sym}:${currentDay}-${hour}-${minute}-60`;
                previousRedisKey = `${sym}:${currentDay}-${hour}-${minute - 1}-60`;
              } else if (minute === 0 && hour > 9) {
                // If the minute is 0, then we need to get the data for the previous hour
                currentRedisKey = `${sym}:${currentDay}-${hour}-${minute}-60`;
                previousRedisKey = `${sym}:${currentDay}-${hour - 1}-59-60`;
              }
              let currentDataParsed = null;
              let previousDataParsed = null;
              let currentData = null;
              let previousData = null;
              if (currentRedisKey && previousRedisKey) {
                // console.log("currentRedisKey", hour, minute);
                try {
                  currentData = await client.get(currentRedisKey);
                  previousData = await client.get(previousRedisKey);
                  currentDataParsed = currentData ? JSON.parse(currentData) : null;
                  previousDataParsed = previousData ? JSON.parse(previousData) : null;
                } catch (error) {
                  console.log("currentRedisKey", currentRedisKey);
                  console.error("Error getting market 30 data:", error);
                }
              } else {
                // console.log("skipping the operation for that minute:", minute, currentRedisKey, previousRedisKey, hour);
                continue;
              }

              if (currentDataParsed && previousDataParsed) {
                const volChange = currentDataParsed.vol_traded_today - previousDataParsed.vol_traded_today;
                const volAmount = Math.round(volChange * currentDataParsed.ltp);
                // If the volume amount is greater than the amount, then set the data in the redis
                for (const lamount of Object.keys(list)) {
                  if (volAmount > lamount) {
                    const redisKey = `${amountKey}:${lamount}:${sym}:${currentDay}-${hour}-${minute}`;
                    const obj = {
                      symbol: sym,
                      ltp: currentDataParsed.ltp,
                      volChange: volChange,
                      amount: volAmount,
                      last_traded_time: currentDataParsed.last_traded_time,
                    };
                    list[lamount][redisKey] = obj;
                    await client.set(redisKey, JSON.stringify(obj), ExpiryTime);
                  }
                }
                if (volHistory && volHistory[sym] && volHistory[sym]["volByDays"] && volHistory[sym]["volByDays"][numberOfDays]) {
                  const avgVolume = volHistory[sym]["volByDays"][numberOfDays].avgVolume;
                  for (const lvol of Object.keys(volList)) {
                    if (volChange > avgVolume * lvol) {
                      const redisKey = `${volKey}:${lvol}:${sym}:${currentDay}-${hour}-${minute}`;
                      const obj = {
                        symbol: sym,
                        ltp: currentDataParsed.ltp,
                        avgVolume: avgVolume,
                        volChange: volChange,
                        amount: volAmount,
                        last_traded_time: currentDataParsed.last_traded_time,
                      };
                      volList[lvol][redisKey] = obj;
                      await client.set(redisKey, JSON.stringify(obj), ExpiryTime);
                    }
                  }
                }
              }
              // list[sym] = {};

              // // Scan the currentRedisKey
              // let cursor = 0;
              // let keys = [];
              // do {
              //   const result = await client.scan(cursor, {
              //     MATCH: currentRedisKey,
              //     COUNT: 100,
              //   });
              //   cursor = result.cursor;
              //   keys = keys.concat(result.keys);
              // } while (cursor !== 0);

              // // Get the data for each key
              // for (const key of keys) {
              //   const value = await client.get(key);
              //   list[sym][key] = value;
              // }
              // Save the data in the file
            }
          } catch (error) {
            console.error("Error getting market 30 data:", error);
          }
        }
        if (hour !== currentHour) {
          // Adding a dummy data for the hour
          await client.set(
            `${amountKey}:${amount}:DUMMY:${currentDay}-${hour}-*`,
            JSON.stringify({
              symbol: "DUMMY",
              ltp: 0,
              volChange: 0,
              amount: 0,
            }),
            ExpiryTime
          );
        }
      } else {
        console.log("skipping the operation for that hour:", hour);
      }
    }

    // Replace the data in the file
    for (const lamount of Object.keys(list)) {
      fs.writeFileSync(`data-${lamount}.json`, JSON.stringify(list[lamount]));
    }
    for (const lvol of Object.keys(volList)) {
      fs.writeFileSync(`vol-${lvol}.json`, JSON.stringify(volList[lvol]));
    }
  } catch (error) {
    console.error("Error calculating by amount:", error);
  } finally {
    isRunning = false;
  }
};

export const getDataByAmount = async (amount) => {
  let list = {};
  try {
    const data = fs.readFileSync(`data-${amount}.json`, "utf8");
    list = JSON.parse(data);
  } catch (error) {
    console.error("Error getting data by amount:", error);
  }
  return list;
};

export const getDataByVol = async (vol, days) => {
  let list = {};
  try {
    const data = fs.readFileSync(`vol-${vol}.json`, "utf8");
    list = JSON.parse(data);
  } catch (error) {
    console.error("Error getting data by vol:", error);
  }
  return list;
};

export const setHistoryData = async (symbol, history) => {
  if (!client) {
    console.error("Redis Client not initialized");
    return;
  }
  const getCurrentTime = new Date();
  const currentDay = getCurrentTime.getDate();
  const redisKey = `history:${currentDay}:${symbol}`;
  await client.set(redisKey, JSON.stringify(history), ExpiryTime);
};

export const getHistoryData = async (symbol) => {
  if (!client) {
    console.error("Redis Client not initialized");
    return;
  }
  const getCurrentTime = new Date();
  const currentDay = getCurrentTime.getDate();
  const redisKey = `history:${currentDay}:${symbol}`;
  const data = await client.get(redisKey);
  return JSON.parse(data);
};

export const getAllHistoryData = async () => {
  if (!client) {
    console.error("Redis Client not initialized");
    return;
  }
  const getCurrentTime = new Date();
  const currentDay = getCurrentTime.getDate();
  const redisKey = `history:${currentDay}:*`;
  let cursor = 0;
  let keys = [];
  do {
    const result = await client.scan(cursor, {
      MATCH: redisKey,
      COUNT: 100,
    });
    cursor = result.cursor;
    keys = keys.concat(result.keys);
  } while (cursor !== 0);
  const data = {};
  for (const key of keys) {
    const value = await client.get(key);
    data[key] = JSON.parse(value);
  }
  return data;
};

export const getVolumeHistoryFromFile = async () => {
  try {
    const getCurrentTime = new Date();
    const currentDay = getCurrentTime.getDate();
    const data = fs.readFileSync(`vol-list-${currentDay}.json`, "utf8");
    return JSON.parse(data);
  } catch (error) {
    console.error("Error getting volume history from file:", error);
    return { error: error };
  }
};

// Binance Trade Data
const redisStreamKey = `binance:trades:`;
export const saveTradeDataToRedis = async (trade, receivedTime) => {
  try {
    // Use XADD to add the trade to the Redis Stream
    // '*' tells Redis to auto-generate a unique ID (timestamp-based)
    // MAXLEN ~ redisStreamMaxLen caps the stream size (approximate)
    const streamData = {
      // Flatten the object for stream fields
      symbol: trade.s,
      tradeId: String(trade.t), // Store IDs as strings
      price: trade.p,
      quantity: trade.q,
      buyerOrderId: String(trade.b),
      sellerOrderId: String(trade.a),
      tradeTime: String(trade.T),
      isBuyerMaker: trade.m ? "1" : "0", // Store boolean as '1' or '0'
      // isBestMatch: trade.M ? '1' : '0', // Often deprecated/not useful, check docs
      receivedTime: String(receivedTime),
    };

    // Add entry to the stream, using tradeId 't' as the message ID for potential deduplication
    // If you prefer Redis-generated IDs ensuring insertion order: use '*' instead of String(trade.t)
    // Using '*' might be safer if trade IDs aren't guaranteed monotonic by the source
    const entryId = await redisClient.xAdd(
      redisStreamKey,
      "*", // Let Redis generate the entry ID
      streamData,
      {
        // XADD options
        TRIM: {
          strategy: "MAXLEN", // Trim by length
          strategyModifier: "~", // Allow approximate trimming for performance
          threshold: redisStreamMaxLen,
        },
      }
    );

    // Optional: Log the generated stream entry ID
    // console.log(`Saved trade ${trade.t} to Redis Stream with ID: ${entryId}`);
  } catch (error) {
    console.error("Error saving data to Redis:", error);
    console.error("Failed data:", trade);
    // Consider how to handle Redis errors (e.g., retry logic, circuit breaker)
    isRedisReady = false; // Assume connection might be lost on error
  }
};

export default client;
