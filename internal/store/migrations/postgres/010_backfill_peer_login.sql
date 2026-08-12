UPDATE peers
SET login = split_part(key, ':', 1)
WHERE login = '';
