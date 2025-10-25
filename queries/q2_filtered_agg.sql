SELECT country, platform, COUNT(*) AS events, SUM(clicks) AS clicks
FROM events
WHERE dt BETWEEN '2025-09-10' AND '2025-09-12'
AND app_id IN (10,11,12,13,14)
AND country IN ('US','GB','DE')
GROUP BY country, platform
ORDER BY country, platform;