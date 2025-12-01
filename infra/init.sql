CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- при желании можно хранить chunks и в PG (дублирование из ES)
-- CREATE TABLE IF NOT EXISTS materials (...);
-- CREATE TABLE IF NOT EXISTS chunks (...);
