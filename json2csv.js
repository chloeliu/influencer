import fs from "fs";
import { Parser } from "json2csv";
import path from "path";

// Get the current directory equivalent to __dirname in ES Modules
import { fileURLToPath } from "url";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Function to convert JSON to CSV
function convertJsonToCsv(jsonFilePath, csvFilePath) {
  // Read the JSON file
  fs.readFile(jsonFilePath, "utf8", (err, data) => {
    if (err) {
      console.error(`Error reading JSON file: ${err}`);
      return;
    }

    try {
      // Parse the JSON data
      const jsonData = JSON.parse(data);

      // Initialize the JSON2CSV parser
      const json2csvParser = new Parser();

      // Convert JSON to CSV
      const csv = json2csvParser.parse(jsonData);

      // Write the CSV data to a file
      fs.writeFile(csvFilePath, csv, (err) => {
        if (err) {
          console.error(`Error writing CSV file: ${err}`);
          return;
        }
        console.log(`CSV file successfully saved to ${csvFilePath}`);
      });
    } catch (parseError) {
      console.error(`Error parsing JSON data: ${parseError}`);
    }
  });
}

// Specify the paths to the JSON and CSV files
const jsonFilePath = path.join(__dirname, "growth_substack_user_profiles_from_publications.json");
const csvFilePath = path.join(__dirname, "growth_substack_user_profiles_from_publications.csv");

// Convert JSON to CSV
convertJsonToCsv(jsonFilePath, csvFilePath);
