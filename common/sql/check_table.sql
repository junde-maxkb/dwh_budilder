SELECT COUNT(*) as count
FROM user_tables
WHERE table_name = UPPER(:table_name)

