SELECT dt, app_id, SUM(revenue) AS revenue, SUM(installs) AS installs
FROM events
WHERE dt BETWEEN '2025-09-05' AND '2025-09-15'
GROUP BY dt, app_id
ORDER BY dt, app_id;