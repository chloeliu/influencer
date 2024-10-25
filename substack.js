import fs from "graceful-fs";
import { gotScraping } from "got-scraping";
import * as cheerio from "cheerio";
import { getProxyUrl } from "./proxies.js";

// Function to build the URL for the latest post request
const buildLatestPostUrl = (userId, offset = 0, limit = 10) => {
  return `https://substack.com/api/v1/profile/posts?profile_user_id=${userId}&offset=${offset}&limit=${limit}`;
};

// Function to build the URL for the popular post request
const buildPopularPostUrl = (userId, offset = 0, limit = 13) => {
  return `https://substack.com/api/v1/profile/posts?profile_user_id=${userId}&offset=${offset}&limit=${limit}`;
};

// Function to build the URL for the publication request
const buildPublicationUrl = (searchQuery, page) => {
  return `https://substack.com/api/v1/publication/search?query=${searchQuery}&page=${page}&lastSearch=1727768370290&skipExplanation=false`;
};

// Optimized retry logic with exponential backoff
const fetchWithRetry = async (url, retries = 3, delay = 1000) => {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await gotScraping(url, {
        responseType: "json",
        proxyUrl: getProxyUrl(),
      });
      return response.body;
    } catch (error) {
      console.warn(`Attempt ${i + 1} failed: ${error.message}`);
      if (i < retries - 1) {
        await new Promise(
          (resolve) => setTimeout(resolve, delay * Math.pow(2, i)) // Exponential backoff
        );
      } else {
        throw error;
      }
    }
  }
};

// Optimized retry logic with exponential backoff
const fetchHTMLWithRetry = async (url, retries = 3, delay = 1000) => {
  for (let i = 0; i < retries; i++) {
    try {
      const response = await gotScraping(url, {
        proxyUrl: getProxyUrl(),
      });
      return response.body;
    } catch (error) {
      console.warn(`Attempt ${i + 1} failed: ${error.message}`);
      if (i < retries - 1) {
        await new Promise(
          (resolve) => setTimeout(resolve, delay * Math.pow(2, i)) // Exponential backoff
        );
      } else {
        throw error;
      }
    }
  }
};

// Function to fetch and parse content from newsletter URLs with retries
const fetchNewsletterContent = async (url) => {
  try {
    const htmlContent = await fetchHTMLWithRetry(url);

    // Load the DOM content into Cheerio
    const $ = cheerio.load(htmlContent);

    // Find the div with class "available-content"
    const availableContentDiv = $(".available-content");

    if (availableContentDiv.length > 0) {
      // Extract texts, images, videos, and URLs
      const texts = [];
      const images = [];
      const videos = [];
      const links = [];

      // Extract all text from <p> tags
      availableContentDiv.find("p").each((i, el) => {
        texts.push($(el).text());
      });

      // Extract all images from <img> tags
      availableContentDiv.find("img").each((i, el) => {
        const imageUrl = $(el).attr("src");
        if (imageUrl) {
          images.push(imageUrl);
        }
      });

      // Extract all video URLs from <video> tags (and optionally <source>)
      availableContentDiv.find("video").each((i, el) => {
        const videoUrl = $(el).attr("src");
        if (videoUrl) {
          videos.push(videoUrl);
        }
      });

      // Extract video sources if they exist
      availableContentDiv.find("source").each((i, el) => {
        const videoSourceUrl = $(el).attr("src");
        if (videoSourceUrl) {
          videos.push(videoSourceUrl);
        }
      });

      // Extract all URLs from <a> tags
      availableContentDiv.find("a").each((i, el) => {
        const linkUrl = $(el).attr("href");
        if (linkUrl) {
          links.push(linkUrl);
        }
      });

      // Return extracted content
      return {
        texts,
        images,
        videos,
        links,
      };
    }

    return {}; // Return empty content if no div with "available-content" found
  } catch (error) {
    console.error(
      `Error fetching or parsing newsletter content from ${url}:`,
      error
    );
    return {};
  }
};

// Modified function to batch process newsletter content fetching
const mapPostDataBatch = async (posts, userId, searchQuery) => {
  // Create a batch of newsletter fetch requests
  const postMapPromises = posts.map(async (post) => {
    console.log(`Fetching content for post ID: ${post.id}`);

    // Initialize variables for podcast and newsletter content
    let podcast_transcription_url = null;
    let newsletter_text = "";
    let newsletter_images = [];
    let newsletter_videos = [];
    let newsletter_links = [];

    // Fetch content based on post type
    if (post.type === "podcast") {
      const cdnUrl = post.podcastUpload?.transcription?.cdn_url || null;
      const transcriptUrl =
        post.podcastUpload?.transcription?.transcript_url || null;
      podcast_transcription_url = cdnUrl || transcriptUrl;
    } else if (post.type === "newsletter") {
      const newsletterContent = await fetchNewsletterContent(
        post.canonical_url
      );
      if (newsletterContent) {
        newsletter_text = (newsletterContent.texts || []).join(" ");
        newsletter_images = newsletterContent.images || [];
        newsletter_videos = newsletterContent.videos || [];
        newsletter_links = newsletterContent.links || [];
      }
    }

    // Return structured post data with conditional fields
    const postData = {
      id: post.id,
      user_id: userId,
      title: post.title,
      type: post.type,
      slug: post.slug,
      post_date: post.post_date,
      audience: post.audience,
      url: post.canonical_url,
      likes: post.reaction_count,
      comments: post.comment_count,
      keyword: searchQuery,
    };

    // Conditionally add podcast transcription URL
    if (post.type === "podcast" && podcast_transcription_url) {
      postData.podcast_transcription_url = podcast_transcription_url;
    }

    // Conditionally add newsletter content fields
    if (post.type === "newsletter") {
      postData.newsletter_text = newsletter_text;
      postData.newsletter_images = newsletter_images;
      postData.newsletter_videos = newsletter_videos;
      postData.newsletter_links = newsletter_links;
    }

    return postData;
  });

  // Wait for all posts in the batch to be processed
  return Promise.all(postMapPromises);
};

// Function to fetch 10 latest posts for multiple users in batches
const fetchUserLatestPostsBatch = async (
  userIds,
  searchQuery,
  batchSize = 10
) => {
  let allUserPosts = [];
  const totalUsers = userIds.length;
  const uniquePostIds = new Set();

  for (let i = 0; i < totalUsers; i += batchSize) {
    const batch = userIds.slice(i, i + batchSize);

    // Use Promise.all to fetch posts for the batch of users concurrently
    const postPromises = batch.map(async (userId) => {
      console.log(`Fetching latest posts for user ID: ${userId}`);
      const postUrl = buildLatestPostUrl(userId);
      try {
        const responseBody = await fetchWithRetry(postUrl);

        // Batch process posts
        const processedPosts = await mapPostDataBatch(
          responseBody.posts,
          userId,
          searchQuery
        );
        return processedPosts;
      } catch (error) {
        console.error(`Error fetching posts for user ${userId}: ${error}`);
        return [];
      }
    });

    // Collect the results of all post fetches in the current batch
    const batchResults = await Promise.all(postPromises);
    const flatBatchResults = batchResults.flat();

    // Filter out duplicate posts based on post ID
    const nonDuplicatePosts = flatBatchResults.filter((post) => {
      if (uniquePostIds.has(post.id)) {
        return false; // Skip if duplicate post ID
      }
      uniquePostIds.add(post.id); // Track new post ID
      return true; // Keep if unique
    });

    allUserPosts.push(...nonDuplicatePosts);
  }

  return allUserPosts;
};

// Function to fetch 10 most popular posts for multiple users
const fetchUserPopularPostsBatch = async (
  userIds,
  searchQuery,
  batchSize = 13
) => {
  let allUserPosts = [];
  const totalUsers = userIds.length;
  const uniquePostIds = new Set();

  // Iterate through users in batches
  for (let i = 0; i < totalUsers; i += batchSize) {
    const batch = userIds.slice(i, i + batchSize);
    console.log(
      `Fetching popular posts for batch ${Math.floor(i / batchSize) + 1}...`
    );

    const postPromises = batch.map(async (userId) => {
      console.log(`Fetching popular posts for user ID: ${userId}`);
      let offset = 0;
      let morePages = true;
      let allPosts = [];
      let totalFetchedPosts = 0;
      const maxPosts = 50; // Set the limit to 50 posts

      // Fetch posts while there are more pages and we haven't reached the limit
      while (morePages && totalFetchedPosts < maxPosts) {
        const postUrl = buildPopularPostUrl(userId, offset);
        try {
          const responseBody = await fetchWithRetry(postUrl);

          // Map each post, and if it's a newsletter, fetch its content
          const processedPosts = await mapPostDataBatch(
            responseBody.posts,
            userId,
            searchQuery
          );

          allPosts.push(...processedPosts);
          totalFetchedPosts += processedPosts.length;

          // Check if there are more pages or if we have reached the max post count
          morePages = responseBody.posts.length === 13;
          offset += 12; // Increment offset for the next page

          // If total fetched posts exceed the limit, stop fetching
          if (totalFetchedPosts >= maxPosts) {
            console.log(
              `Reached the post limit of ${maxPosts} posts for user ${userId}`
            );
            morePages = false;
          }
        } catch (error) {
          console.error(
            `Error fetching popular posts for user ${userId}: ${error}`
          );
          break;
        }
      }

      const topPosts = allPosts.sort((a, b) => b.likes - a.likes).slice(0, 10);
      return topPosts;
    });

    const batchResults = await Promise.all(postPromises);
    const flatBatchResults = batchResults.flat();

    // Filter out duplicate posts based on post ID
    const nonDuplicatePosts = flatBatchResults.filter((post) => {
      if (uniquePostIds.has(post.id)) {
        return false;
      }
      uniquePostIds.add(post.id);
      return true;
    });

    allUserPosts.push(...nonDuplicatePosts);
  }

  console.log(`Completed fetching popular posts for ${totalUsers} users.`);
  return allUserPosts;
};

// Function to fetch a batch of publication results
const fetchPublicationBatch = async (searchQuery, currentPage, batchSize) => {
  const batchPromises = [];
  for (let i = 0; i < batchSize; i++) {
    const url = buildPublicationUrl(searchQuery, currentPage + i);
    console.log(`Queuing publication request for page ${currentPage + i}...`);
    batchPromises.push(fetchWithRetry(url));
  }

  const batchResults = await Promise.all(batchPromises);

  const allMappedResults = [];
  let morePages = true;

  // Iterate over the fetched batch results
  batchResults.forEach((fetchJson, index) => {
    const currentBatchPage = currentPage + index;

    // Check if results exist and proceed to filter
    if (fetchJson && fetchJson.results) {
      // Filter out publications with freeSubscriberCount >= 5000
      const filteredResults = fetchJson.results.filter((item) => {
        if (item.freeSubscriberCount) {
          const subscriberCountNumber = parseInt(
            item.freeSubscriberCount.replace(/,/g, ""),
            10
          );
          return subscriberCountNumber >= 5000;
        }
        return false;
      });

      // Map filtered results to include author_id, type, description, and subscribers
      const mappedResults = filteredResults.map((item) => ({
        id: item.author_id,
        name: item.copyright,
        description: item.bio,
        url: item.base_url,
        subscribers: parseInt(item.freeSubscriberCount.replace(/,/g, ""), 10),
        author_name: item.author_name,
        author_handle: item.author_handle,
        author_photo_url: item.author_photo_url,
        author_bio: item.author_bio,
        twitter_screen_name: item.twitter_screen_name,
      }));

      allMappedResults.push(...mappedResults);

      console.log(
        `Page ${currentBatchPage} fetched successfully. Filtered ${filteredResults.length} publications.`
      );
    } else {
      console.error(`Error: No results found for page ${currentBatchPage}`);
    }

    // Determine if there are more pages to fetch
    morePages = fetchJson?.more || false;
  });

  return { allMappedResults, morePages };
};

// Function to write results to a file
const writeResultsToFile = (filename, data) => {
  fs.writeFileSync(filename, JSON.stringify(data, null, 2));
  console.log(`Data saved to ${filename}`);
};

// Function to scrape publication data for multiple queries
const scrapePublicationDataForQueries = async (searchQueries, batchSize) => {
  for (const searchQuery of searchQueries) {
    console.log(
      `Starting to scrape publication data for query: "${searchQuery}"`
    );
    let currentPage = 0;
    let morePages = true;
    const allFilteredResults = [];
    const uniqueIds = new Set();

    // Fetch all publication data in batches
    while (morePages) {
      try {
        const { allMappedResults, morePages: nextPageExists } =
          await fetchPublicationBatch(searchQuery, currentPage, batchSize);

        // Filter duplicates by checking the 'id'
        const nonDuplicateResults = allMappedResults.filter((result) => {
          if (uniqueIds.has(result.id)) {
            return false;
          }
          uniqueIds.add(result.id);
          return true;
        });

        allFilteredResults.push(...nonDuplicateResults);
        currentPage += batchSize;
        morePages = nextPageExists;
      } catch (error) {
        console.error(
          `Error fetching publication data on page ${currentPage}:`,
          error
        );
        break;
      }
    }

    console.log(
      `Fetching complete. Total filtered publications: ${allFilteredResults.length}.`
    );

    writeResultsToFile(
      `${searchQuery}_substack_user_profiles_from_publications.json`,
      allFilteredResults
    );

    // Extract author IDs for fetching posts
    const userIds = allFilteredResults.map((author) => author.id);

    // Fetch latest posts for users in batches
    const allUserLatestPosts = await fetchUserLatestPostsBatch(
      userIds,
      searchQuery,
      10
    );
    writeResultsToFile(
      `${searchQuery}_substack_all_users_latest_posts.json`,
      allUserLatestPosts
    );

    // Fetch popular posts for users in batches
    const allUserPopularPosts = await fetchUserPopularPostsBatch(
      userIds,
      searchQuery,
      10
    );
    writeResultsToFile(
      `${searchQuery}_substack_all_users_popular_posts.json`,
      allUserPopularPosts
    );
  }
};

// Self-invoking function to start the scrape process
(async () => {
  const searchQueries = ["growth"];
  const batchSize = 50;

  await scrapePublicationDataForQueries(searchQueries, batchSize);
})();
