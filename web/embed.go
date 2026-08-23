// Package web embeds the built-in console assets so the single gateway
// binary can serve them regardless of its working directory.
package web

import "embed"

//go:embed index.html app.js styles.css
var FS embed.FS
