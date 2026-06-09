package store

import (
	"strconv"
	"strings"
)

func normalizeHash(h string) string {
	return strings.ToLower(strings.TrimSpace(h))
}

func chunkKey(fileID string, index int) string {
	return fileID + "#" + strconv.Itoa(index)
}
