SELECT
  patient_count,
  AVG(death_count) as avg_death_count
FROM COVID_data.trips
GROUP BY patient_count
ORDER BY avg_death_count DESC;
