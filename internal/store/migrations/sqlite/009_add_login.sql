ALTER TABLE peers ADD COLUMN login TEXT NOT NULL DEFAULT '';
UPDATE peers
SET login = CASE
    WHEN instr(key, ':') > 0 THEN substr(key, 1, instr(key, ':') - 1)
    ELSE key
END
WHERE login = '';
