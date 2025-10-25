SELECT e.dt, a.category, SUM(e.revenue) AS revenue, SUM(e.installs) AS installs
FROM events e
JOIN apps_dim a ON e.app_id = a.app_id
WHERE e.dt BETWEEN '2025-09-01' AND '2025-09-21'
GROUP BY e.dt, a.category
ORDER BY e.dt, a.category;