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
  model: "gemini-2.0-flash",
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

// ------helper functions---------
function getCell(worksheet, cIndex, rIndex) {
  const ref = XLSX.utils.encode_cell({ c: cIndex, r: rIndex });
  return worksheet[ref];
}

// Find a column by header label (row 1), supporting exact strings or regex.
// Returns a column index or null.
function findColumnByHeader(worksheet, candidates) {
  if (!worksheet["!ref"]) return null;
  const range = XLSX.utils.decode_range(worksheet["!ref"]);
  const headerRow = range.s.r;

  // Collect header texts for row 1
  const headers = [];
  for (let c = range.s.c; c <= range.e.c; c++) {
    const cell = getCell(worksheet, c, headerRow);
    const raw = cell && typeof cell.v === "string" ? cell.v.trim() : null;
    if (raw) headers.push({ text: raw, lower: raw.toLowerCase(), col: c });
  }

  // Normalize candidates: string (case-insensitive) or RegExp
  const norm = (x) =>
    typeof x === "string"
      ? { type: "str", value: x.toLowerCase() }
      : { type: "re", value: x };
  const wants = (candidates || []).map(norm);

  // pass 1: exact case-insensitive matches
  for (const w of wants) {
    if (w.type === "str") {
      const hit = headers.find((h) => h.lower === w.value);
      if (hit) return hit.col;
    }
  }
  // pass 2: regex matches against the original header text
  for (const w of wants) {
    if (w.type === "re") {
      const hit = headers.find((h) => w.value.test(h.text));
      if (hit) return hit.col;
    }
  }
  return null;
}

// Decide source/target columns for a single-sheet workbook using your two patterns
function resolveColumnsForSingleSheet(worksheet) {
  // Constellation (source) OR Cosom (source)
  const sourceColIndex =
    findColumnByHeader(worksheet, ["Field Value", "Base", "Source", "Text"]) ??
    1; // fallback: column B

  // Constellation (target): "Translated string <locale>" — use regex
  // Cosom (target): "Translation"
  const targetColIndex =
    findColumnByHeader(worksheet, [
      /^Translated string\b/i,
      "Translation",
      "Translated",
      "Norsk",
    ]) ?? 3; // fallback: column D

  return { sourceColIndex, targetColIndex };
}

//-------------------------------

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
 * 3. Translates unique texts in batches using translateBatch.
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
  // ---updated detect columns
  // We assume ONE sheet (your case). If there are more, we’ll just use the first.
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  if (!worksheet || !worksheet["!ref"]) {
    throw new Error("No data range found in the first sheet.");
  }

  // Detect source/target columns by headers
  const { sourceColIndex, targetColIndex } =
    resolveColumnsForSingleSheet(worksheet);
  console.log(
    `Detected columns on '${sheetName}': source=${sourceColIndex}, target=${targetColIndex}`
  );

  //-------------

  // Step 1: Collect rows from column B and also build a unique set of texts.
  // let rowsToTranslate = [];
  // let uniqueTexts = new Set();
  // workbook.SheetNames.forEach((sheetName) => {
  //   const worksheet = workbook.Sheets[sheetName];
  //   if (!worksheet["!ref"]) return;
  //   const range = XLSX.utils.decode_range(worksheet["!ref"]);
  //   for (let r = range.s.r + 1; r <= range.e.r; r++) {
  //     const cellBRef = XLSX.utils.encode_cell({ c: 1, r });
  //     const cellB = worksheet[cellBRef];
  //     if (
  //       cellB &&
  //       cellB.t === "s" &&
  //       typeof cellB.v === "string" &&
  //       cellB.v.trim()
  //     ) {
  //       const original = cellB.v.trim();
  //       rowsToTranslate.push({
  //         sheetName,
  //         row: r,
  //         original,
  //       });
  //       uniqueTexts.add(original);
  //     }
  //   }
  // });
  // console.log(
  //   `Total rows to process: ${rowsToTranslate.length}; Unique texts: ${uniqueTexts.size}`
  // );

  //----updated detect column
  // Collect rows & unique texts starting from row after header
  let rowsToTranslate = [];
  let uniqueTexts = new Set();
  const range = XLSX.utils.decode_range(worksheet["!ref"]);
  for (let r = range.s.r + 1; r <= range.e.r; r++) {
    const cell = getCell(worksheet, sourceColIndex, r);
    if (cell && cell.t === "s" && typeof cell.v === "string" && cell.v.trim()) {
      const original = cell.v.trim();
      rowsToTranslate.push({ sheetName, row: r, original, targetColIndex });
      uniqueTexts.add(original);
    }
  }
  console.log(
    `Total rows to process: ${rowsToTranslate.length}; Unique texts: ${uniqueTexts.size}`
  );
  //-------------------------

  // Step 2: Translate unique texts in batches.
  const translationCache = {}; // Cache: original text => translated text
  const uniqueList = Array.from(uniqueTexts);
  const batchSize = 100;
  for (let i = 0; i < uniqueList.length; i += batchSize) {
    const batch = uniqueList.slice(i, i + batchSize);
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
${batch.join("\n")}`;
    console.log(`Translating unique batch ${Math.floor(i / batchSize) + 1}`);
    let translatedBatch;
    try {
      translatedBatch = await translateBatch(prompt);
      if (translatedBatch.length !== batch.length) {
        console.warn(
          `Unique batch translation count mismatch: expected ${batch.length} but got ${translatedBatch.length}`
        );
      }
    } catch (err) {
      console.error(
        `Error translating unique batch starting at index ${i}:`,
        err
      );
      // Mark translations as empty for this batch.
      batch.forEach((original) => {
        translationCache[original] = "";
      });
      continue;
    }
    // Update cache with translations.
    translatedBatch.forEach((translationObj) => {
      const key = translationObj.key.trim();
      translationCache[key] = translationObj.value;
    });
    // Optionally update progress based on unique translations.
    const processedUnique = Math.min(uniqueList.length, i + batchSize);
    const progress = Math.round((processedUnique / uniqueList.length) * 100);
    console.log(`Updating progress to ${progress}% for unique translations`);
    await supabase
      .from("jobs")
      .update({ progress, updated_at: new Date().toISOString() })
      .eq("job_id", jobId);
  }

  // Step 3: Assign translations to each row using the cache.
  rowsToTranslate.forEach((item) => {
    item.translated = translationCache[item.original] || "";
  });

  // Step 4: Write translations into column D.
  // rowsToTranslate.forEach((item) => {
  //   const worksheet = workbook.Sheets[item.sheetName];
  //   if (!worksheet) return;
  //   const cellDRef = XLSX.utils.encode_cell({ c: 3, r: item.row });
  //   worksheet[cellDRef] = { t: "s", v: item.translated || "" };
  // });
  // ---updated-----
  rowsToTranslate.forEach((item) => {
    const ws = workbook.Sheets[item.sheetName];
    if (!ws) return;
    const ref = XLSX.utils.encode_cell({ c: item.targetColIndex, r: item.row });
    ws[ref] = { t: "s", v: item.translated || "" };
  });
  // ---------------

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
