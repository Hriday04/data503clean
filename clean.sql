
START TRANSACTION;

CREATE TABLE IF NOT EXISTS final.scrapes (
    scrape_id SERIAL PRIMARY KEY,
    category_tag TEXT NOT NULL,
    scraped_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS final.companies (
    company_id SERIAL PRIMARY KEY,
    canonical_name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS final.scrape_results (
    result_id SERIAL PRIMARY KEY,
    scrape_id INTEGER REFERENCES final.scrapes(scrape_id) ON DELETE CASCADE,
    company_id INTEGER REFERENCES final.companies(company_id) ON DELETE CASCADE,
    job_count INTEGER NOT NULL
);

WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY category_tag,
    date_trunc('hour', scraped_at) - (EXTRACT(HOUR FROM scraped_at)::int % 12) * INTERVAL '1 hour',
    raw_json
               ORDER BY scraped_at
           ) AS rk
    FROM final.adzuna_top_companies_raw
    WHERE normalized = false
)
DELETE FROM final.adzuna_top_companies_raw
WHERE id IN (
    SELECT id FROM ranked WHERE rk > 1
);

INSERT INTO final.scrapes (category_tag, scraped_at)
SELECT DISTINCT category_tag, scraped_at
FROM final.adzuna_top_companies_raw
WHERE normalized = false;

INSERT INTO final.companies (canonical_name)
SELECT DISTINCT jsonb_array_elements(raw_json->'leaderboard')->>'canonical_name' AS canonical_name
FROM final.adzuna_top_companies_raw
WHERE normalized = false AND raw_json->'leaderboard' IS NOT NULL;

INSERT INTO final.scrape_results (scrape_id, company_id, job_count)
SELECT s.scrape_id,
       c.company_id,
       (j->>'count')::INT AS job_count
FROM final.adzuna_top_companies_raw r
JOIN final.scrapes s ON s.category_tag = r.category_tag AND s.scraped_at = r.scraped_at,
     LATERAL jsonb_array_elements(r.raw_json->'leaderboard') AS j
JOIN final.companies c ON c.canonical_name = j->>'canonical_name'
WHERE r.normalized = false;

UPDATE final.adzuna_top_companies_raw
SET normalized = true
WHERE normalized = false;


COMMIT;
