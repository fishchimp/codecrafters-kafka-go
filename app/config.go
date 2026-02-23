package main

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const clusterMetadataDirName = "__cluster_metadata-0"

func discoverClusterMetadataLogPaths(args []string) []string {
	if len(args) >= 2 {
		if logDir, ok := readFirstLogDirFromProperties(args[1]); ok {
			paths := listMetadataLogSegments(filepath.Join(logDir, clusterMetadataDirName))
			if len(paths) > 0 {
				return paths
			}
		}
	}
	return []string{fallbackClusterMetadataLogPath}
}

func readFirstLogDirFromProperties(path string) (string, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}

	lines := strings.Split(string(data), "\n")
	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if key != "log.dirs" {
			continue
		}
		value := strings.TrimSpace(parts[1])
		for _, entry := range strings.Split(value, ",") {
			entry = strings.TrimSpace(entry)
			if entry != "" {
				return entry, true
			}
		}
		return "", false
	}
	return "", false
}

func listMetadataLogSegments(metadataDir string) []string {
	entries, err := os.ReadDir(metadataDir)
	if err != nil {
		return nil
	}

	segments := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		stem := strings.TrimSuffix(name, ".log")
		if len(stem) != 20 {
			continue
		}
		allDigits := true
		for _, ch := range stem {
			if ch < '0' || ch > '9' {
				allDigits = false
				break
			}
		}
		if !allDigits {
			continue
		}
		segments = append(segments, filepath.Join(metadataDir, name))
	}

	sort.Strings(segments)
	return segments
}
