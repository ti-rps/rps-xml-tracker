// Package migrations embeds the goose SQL migrations so the api can apply them
// on startup (no goose CLI needed on the production server).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
