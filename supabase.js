import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
dotenv.config();

// Initialize Supabase connection
const supabaseUrl = `${process.env.SUPABASE_URL}`;
const supabaseKey = `${process.env.SUPABASE_KEY}`;
const supabase = createClient(supabaseUrl, supabaseKey);

// File paths for the JSON files
const userProfilesPath = path.resolve(
  "growth_substack_user_profiles_from_publications.json"
);
const latestPostsPath = path.resolve(
  "growth_substack_all_users_latest_posts.json"
);
const popularPostsPath = path.resolve(
  "growth_substack_all_users_popular_posts.json"
);

let hasError = false;

const deduplicatePosts = (posts) => {
  const seen = new Set();
  return posts.filter((post) => {
    if (seen.has(post.id)) {
      return false; // skip duplicate
    }
    seen.add(post.id);
    return true; // keep unique post
  });
};

// Function to insert users into the database with detailed reporting
const insertUsers = async (users) => {
  console.log(`Inserting ${users.length} user(s) into the database...`);

  const { data, error } = await supabase.from("users").insert(users);

  if (error) {
    console.error("Error inserting users:", error.message);
    hasError = true;
  } else {
    console.log("User(s) inserted successfully.");
  }
};

const upsertPosts = async (posts, postType) => {
  console.log(
    `Upserting ${posts.length} ${postType} post(s) into the database...`
  );

  // Deduplicate posts before upserting
  const uniquePosts = deduplicatePosts(posts);

  // Use upsert to handle duplicate records
  const { data, error } = await supabase
    .from("posts")
    .upsert(uniquePosts, { onConflict: ["id"] });

  if (error) {
    console.error(`Error upserting ${postType} posts:`, error.message);
    hasError = true;
  } else {
    console.log("Post(s) upserted successfully.");
  }
};

// Function to read the JSON files and save the data into Supabase
const readAndSaveData = async () => {
  try {
    // Read and process the user profiles JSON file
    console.log(`Reading user profiles from ${userProfilesPath}...`);
    const userProfilesData = fs.readFileSync(userProfilesPath, "utf8");
    const userProfiles = JSON.parse(userProfilesData);
    console.log(`Loaded ${userProfiles.length} user profile(s).`);

    // Read and process the latest posts JSON file
    console.log(`Reading latest posts from ${latestPostsPath}...`);
    const latestPostsData = fs.readFileSync(latestPostsPath, "utf8");
    const latestPosts = JSON.parse(latestPostsData);
    console.log(`Loaded ${latestPosts.length} latest post(s).`);

    // Read and process the popular posts JSON file
    console.log(`Reading popular posts from ${popularPostsPath}...`);
    const popularPostsData = fs.readFileSync(popularPostsPath, "utf8");
    const popularPosts = JSON.parse(popularPostsData);
    console.log(`Loaded ${popularPosts.length} popular post(s).`);

    // Insert users into the database
    await insertUsers(userProfiles);

    // Upsert posts (latest and popular) to handle duplicates
    await upsertPosts(latestPosts, "latest");
    await upsertPosts(popularPosts, "popular");

    // At the end
    if (hasError) {
      console.error("Data insertion completed with some errors.");
    } else {
      console.log("All data inserted successfully.");
    }
  } catch (error) {
    console.error(
      "Error during file reading or data insertion:",
      error.message
    );
  }
};

// Main function to read JSON files and save data
(async () => {
  console.log("Starting data insertion process...");
  await readAndSaveData();
})();
