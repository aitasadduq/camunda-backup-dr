package web

import (
	"io/fs"
	"strings"
	"testing"
)

func TestFS_ContainsIndexHTML(t *testing.T) {
	data, err := fs.ReadFile(FS, "index.html")
	if err != nil {
		t.Fatalf("expected index.html to be embedded, got error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("index.html is embedded but empty")
	}
	content := string(data)
	if !strings.Contains(content, "<!DOCTYPE html>") {
		t.Error("index.html does not contain <!DOCTYPE html>")
	}
	if !strings.Contains(content, "Camunda Backup Controller") {
		t.Error("index.html does not contain expected title")
	}
}

func TestFS_ContainsCSS(t *testing.T) {
	data, err := fs.ReadFile(FS, "css/styles.css")
	if err != nil {
		t.Fatalf("expected css/styles.css to be embedded, got error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("css/styles.css is embedded but empty")
	}
}

func TestFS_ContainsJS(t *testing.T) {
	data, err := fs.ReadFile(FS, "js/app.js")
	if err != nil {
		t.Fatalf("expected js/app.js to be embedded, got error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("js/app.js is embedded but empty")
	}
}

func TestFS_DirectoryStructure(t *testing.T) {
	expectedFiles := []string{
		"index.html",
		"css/styles.css",
		"js/app.js",
	}

	for _, path := range expectedFiles {
		_, err := fs.Stat(FS, path)
		if err != nil {
			t.Errorf("expected %s to exist in embedded FS, got error: %v", path, err)
		}
	}
}

func TestFS_NoGoFilesEmbedded(t *testing.T) {
	_, err := fs.ReadFile(FS, "embed.go")
	if err == nil {
		t.Error("embed.go should not be present in the embedded filesystem")
	}
}

func TestFS_CSSDirectoryEntries(t *testing.T) {
	entries, err := fs.ReadDir(FS, "css")
	if err != nil {
		t.Fatalf("failed to read css directory: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("css directory is empty")
	}

	found := false
	for _, e := range entries {
		if e.Name() == "styles.css" {
			found = true
			break
		}
	}
	if !found {
		t.Error("styles.css not found in css directory")
	}
}

func TestFS_JSDirectoryEntries(t *testing.T) {
	entries, err := fs.ReadDir(FS, "js")
	if err != nil {
		t.Fatalf("failed to read js directory: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("js directory is empty")
	}

	found := false
	for _, e := range entries {
		if e.Name() == "app.js" {
			found = true
			break
		}
	}
	if !found {
		t.Error("app.js not found in js directory")
	}
}
