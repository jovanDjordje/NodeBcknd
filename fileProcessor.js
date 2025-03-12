// fileProcessor.js
const { createClient } = require("@supabase/supabase-js");
const { Storage } = require("@google-cloud/storage");
const XLSX = require("xlsx");
const { GoogleGenerativeAI } = require("@google/generative-ai");
const { storageClient } = require("./gcpStorage");
require("dotenv").config();

// Setup Gemini API client.
const apiKey = process.env.GOOGLE_API_KEY;
if (!apiKey) {
  throw new Error("GOOGLE_API_KEY environment variable is not set.");
}
const genAI = new GoogleGenerativeAI(apiKey);
const model = genAI.getGenerativeModel({
  model: "gemini-1.5-flash-8b",
});
const generationConfig = {
  temperature: 1,
  topP: 0.95,
  topK: 40,
  maxOutputTokens: 8192,
  responseMimeType: "text/plain",
};

// Use the provided storageClient or create a new one.
const storage = storageClient || new Storage();
const bucketName = process.env.GCLOUD_BUCKET;
if (!bucketName) {
  throw new Error("GCLOUD_BUCKET environment variable is not set.");
}

// Initialize Supabase client.
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!supabaseUrl || !supabaseKey) {
  throw new Error("Supabase environment variables are not set.");
}
const supabase = createClient(supabaseUrl, supabaseKey);

/**
 * Calls Gemini’s chat API to translate a batch of strings.
 */
async function translateBatch(prompt) {
  console.log("Calling Gemini API with prompt:\n", prompt);
  const chatSession = await model.startChat({
    generationConfig,
    history: [
      {
        role: "user",
        parts: [{ text: prompt }],
      },
    ],
  });
  // Send a follow-up message using the same prompt as trigger.
  const result = await chatSession.sendMessage(prompt);
  const rawOutput = result.response.text();
  const cleanedOutput = rawOutput
    .replace(/<think>[\s\S]*?<\/think>/, "")
    .trim();
  const match = cleanedOutput.match(/\[[\s\S]*\]/);
  if (!match) {
    throw new Error(
      "Failed to extract JSON array from output: " + cleanedOutput
    );
  }
  let jsonString = match[0];
  jsonString = jsonString.replace(/,\s*([}\]])/g, "$1");
  let jsonOutput;
  try {
    jsonOutput = JSON.parse(jsonString);
  } catch (e) {
    throw new Error("Failed to parse JSON output: " + e.message);
  }
  if (!Array.isArray(jsonOutput)) {
    throw new Error("JSON output is not an array");
  }
  return jsonOutput;
}

/**
 * Processes the Excel file:
 * 1. Downloads the file from Cloud Storage.
 * 2. Reads the workbook and collects rows from column B.
 * 3. Translates them in batches using translateBatch.
 * 4. Writes translations into column D.
 * 5. Uploads the updated workbook back to Cloud Storage and returns a signed URL.
 */
async function processFile(fileUrl, jobId) {
  // Extract the file name from the URL.
  const urlParts = fileUrl.split("/");
  const fileName = urlParts[urlParts.length - 1];
  const bucket = storage.bucket(bucketName);
  const file = bucket.file(fileName);

  // Get job details from Supabase.
  const { data: job, error } = await supabase
    .from("jobs")
    .select("custom_comments")
    .eq("job_id", jobId)
    .single();
  if (error) {
    throw new Error(error.message);
  }
  const customComments = job && job.custom_comments ? job.custom_comments : "";
  const customHeader = customComments
    ? `\nAdditional instructions: ${customComments}`
    : "";

  // Download the file.
  const [fileBuffer] = await file.download();
  console.log(`Downloaded file ${fileName}`);

  // Read the workbook.
  const workbook = XLSX.read(fileBuffer, { type: "buffer" });
  console.log("Workbook read. Sheets:", workbook.SheetNames.join(", "));

  // Collect rows from column B (skip header row).
  let rowsToTranslate = [];
  workbook.SheetNames.forEach((sheetName) => {
    const worksheet = workbook.Sheets[sheetName];
    if (!worksheet["!ref"]) return;
    const range = XLSX.utils.decode_range(worksheet["!ref"]);
    for (let r = range.s.r + 1; r <= range.e.r; r++) {
      const cellBRef = XLSX.utils.encode_cell({ c: 1, r });
      const cellB = worksheet[cellBRef];
      if (
        cellB &&
        cellB.t === "s" &&
        typeof cellB.v === "string" &&
        cellB.v.trim()
      ) {
        rowsToTranslate.push({
          sheetName,
          row: r,
          original: cellB.v.trim(),
        });
      }
    }
  });

  console.log(
    `Total rows to translate (from column B): ${rowsToTranslate.length}`
  );
  const totalRows = rowsToTranslate.length;
  const batchSize = 100;
  for (let i = 0; i < rowsToTranslate.length; i += batchSize) {
    const batch = rowsToTranslate.slice(i, i + batchSize);
    const prompt = `Translate the lines of text after delimiter "-->" to Norwegian.${customHeader}.
Respond with a JSON array where each entry is an object with two keys:
  "key": the original text exactly as provided,
  "value": the Norwegian translation.
For example, if the input is:
Select...
New
The output should be:
[
  {"key": "Select...", "value": "Velg..."},
  {"key": "New", "value": "Nytt"}
]
Do not output any additional text.
Here are the strings -->
${batch.map((item) => item.original).join("\n")}`;
    console.log(`Translating batch ${Math.floor(i / batchSize) + 1}`);
    let translatedBatch;
    try {
      translatedBatch = await translateBatch(prompt);
      if (translatedBatch.length !== batch.length) {
        console.warn(
          `Batch translation count mismatch: expected ${batch.length} but got ${translatedBatch.length}`
        );
      }
    } catch (err) {
      console.error(`Error translating batch starting at index ${i}:`, err);
      batch.forEach((item) => (item.translated = ""));
      continue;
    }
    batch.forEach((item) => {
      const match = translatedBatch.find(
        (obj) => obj.key.trim() === item.original
      );
      item.translated = match ? match.value : "";
      if (!match) {
        console.warn(`No matching translation found for "${item.original}"`);
      }
    });
    // Update progress in Supabase.
    const processedRows = Math.min(totalRows, i + batchSize);
    const progress = Math.round((processedRows / totalRows) * 100);
    console.log(`Updating progress to ${progress}%`);
    await supabase
      .from("jobs")
      .update({ progress, updated_at: new Date().toISOString() })
      .eq("job_id", jobId);
  }

  // Write translations into column D.
  rowsToTranslate.forEach((item) => {
    const worksheet = workbook.Sheets[item.sheetName];
    if (!worksheet) return;
    const cellDRef = XLSX.utils.encode_cell({ c: 3, r: item.row });
    worksheet[cellDRef] = { t: "s", v: item.translated || "" };
  });

  // Write the updated workbook to a Buffer.
  const outBuffer = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });
  const processedFileName = `processed-${fileName}`;
  const processedFile = bucket.file(processedFileName);
  await processedFile.save(outBuffer, {
    contentType:
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });
  console.log(`Processed file uploaded as ${processedFileName}`);

  // Generate a signed URL for the processed file.
  const [signedUrl] = await processedFile.getSignedUrl({
    action: "read",
    expires: Date.now() + 10 * 60 * 1000, // Valid for 10 minutes.
  });
  return signedUrl;
}

module.exports = { processFile };
