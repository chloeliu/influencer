-- Create the users table
CREATE TABLE IF NOT EXISTS users (
  id bigint PRIMARY KEY,
  name text,
  url text,
  subscribers int
);

-- Create the posts table
CREATE TABLE IF NOT EXISTS posts (
  id bigint PRIMARY KEY,
  user_id bigint REFERENCES users(id),
  title text,
  type text,
  slug text,
  post_date timestamp,
  audience text,
  url text,
  likes int,
  comments int,
  keyword text,
  newsletter_text text,
  podcast_transcription_url text,
  newsletter_links text[],
  newsletter_videos text[],
  newsletter_images text[]
);

-- Fetch the 10 Latest Posts for All Users
SELECT *
FROM posts
WHERE user_id IN (SELECT id FROM users)
ORDER BY post_date DESC
LIMIT 10;

-- Fetch lastest 10 posts for each user
WITH ranked_posts AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY post_date DESC) AS rn
  FROM posts
)
SELECT *
FROM ranked_posts
WHERE rn <= 10;

-- Fetch the 10 Most Popular Posts for Each User Based on "Likes"
WITH ranked_posts AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY likes DESC) AS rn
  FROM posts
)
SELECT *
FROM ranked_posts
WHERE rn <= 10;

-- Fetch Latest 10 Newsletter Posts for Each User
SELECT *
FROM (
    SELECT p.*, 
           ROW_NUMBER() OVER (PARTITION BY p.user_id ORDER BY p.post_date DESC) AS rn
    FROM posts p
    WHERE p.type = 'newsletter'
) subquery
WHERE rn <= 10
ORDER BY user_id, post_date DESC;

-- Fetch Latest 10 Podcast Posts for Each User
SELECT *
FROM (
    SELECT p.*, 
           ROW_NUMBER() OVER (PARTITION BY p.user_id ORDER BY p.post_date DESC) AS rn
    FROM posts p
    WHERE p.type = 'podcast'
) subquery
WHERE rn <= 10
ORDER BY user_id, post_date DESC;

