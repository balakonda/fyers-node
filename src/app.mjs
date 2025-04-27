import express from "express";
import bodyParser from "body-parser";
import fyersRoutes from "./routes/fyers.routes.mjs";
import cors from "cors";

const app = express();
app.use(bodyParser.json());
app.use(cors());
// Use the modular routes
app.use("/api/", fyersRoutes);

// Start the server
const PORT = 5000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
