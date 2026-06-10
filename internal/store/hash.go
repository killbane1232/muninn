package store

import (
	"strconv"
	"strings"
)

func normalizeHash(h string) string {
	return strings.ToLower(strings.TrimSpace(h))
}

func validHashFormat(h string) bool {
	if len(h) != 16 {
		return false
	}
	for _, c := range h {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func chunkKey(fileID string, index int) string {
	return fileID + "#" + strconv.Itoa(index)
}
