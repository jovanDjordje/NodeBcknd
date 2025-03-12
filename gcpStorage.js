// gcpStorage.js
const { Storage } = require("@google-cloud/storage");

// Creates a new Storage client using credentials from your environment.
const storageClient = new Storage();

module.exports = { storageClient };
