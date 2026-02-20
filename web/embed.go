package web

import "embed"

// FS embeds all static web assets (index.html, css/, js/).
//
//go:embed index.html css js
var FS embed.FS
