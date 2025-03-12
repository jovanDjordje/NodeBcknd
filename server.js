const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
require("dotenv").config();
const { processFile } = require("./fileProcessor");
const { createClient } = require("@supabase/supabase-js");

const app = express();
app.use(bodyParser.json());
app.use(cors({ origin: "http://localhost:3000" })); // Enable CORS for local testing

const PORT = process.env.PORT || 1000;
const API_KEY = process.env.API_KEY || "your-secret-api-key";

// Initialize Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

function authenticate(req, res, next) {
  const key = req.headers["x-api-key"];
  if (!key || key !== API_KEY) {
    console.error("Unauthorized access attempt");
    return res.status(401).json({ error: "Unauthorized" });
  }
  console.log("Authentication successful");
  next();
}

// Processing endpoint
app.post("/process-file", authenticate, async (req, res) => {
  console.log("Received request to process file");

  try {
    const { fileUrl, jobId } = req.body;
    if (!fileUrl || !jobId) {
      console.error("Invalid request body:", req.body);
      return res.status(400).json({ error: "Missing fileUrl or jobId" });
    }

    console.log(
      `Starting processing for jobId: ${jobId} with file: ${fileUrl}`
    );

    // **Immediately update Supabase status**
    await supabase
      .from("jobs")
      .update({ status: "processing", updated_at: new Date().toISOString() })
      .eq("job_id", jobId);

    // **Return response immediately and close connection**
    res.status(202).json({ message: "Processing started", jobId });
    res.end(); // 🔥 **Explicitly close the response**

    // **Run processing in the background**
    processFile(fileUrl, jobId)
      .then(async (processedFileUrl) => {
        console.log(
          `Processing complete for job ${jobId}: ${processedFileUrl}`
        );
        await supabase
          .from("jobs")
          .update({
            status: "completed",
            processed_file_url: processedFileUrl,
            updated_at: new Date().toISOString(),
          })
          .eq("job_id", jobId);
      })
      .catch(async (err) => {
        console.error(`Error processing job ${jobId}:`, err);
        await supabase
          .from("jobs")
          .update({ status: "error", updated_at: new Date().toISOString() })
          .eq("job_id", jobId);
      });
  } catch (error) {
    console.error("Unexpected error in processing:", error);
    res.status(500).json({ error: "Internal Server Error" });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});

server.keepAliveTimeout = 350000;
server.headersTimeout = 350000;
