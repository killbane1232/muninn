ALTER TABLE peers ADD COLUMN IF NOT EXISTS login TEXT NOT NULL DEFAULT '';
UPDATE peers
SET login = split_part(key, ':', 1)
WHERE login = '';
