package webui

import (
	_ "embed"
	"net/http"
)

// The settings page the server carries itself.
//
// The single-page application is shipped as a built bundle and its source is
// not in this repository, so a page cannot be added to it here. This one is
// served by the binary instead, at a path of its own, and it survives the
// bundle being rebuilt.
//
// It is not a second way in. It reads the same accessToken the application
// stores and sends the same bearer header, so it reaches the same endpoints
// under the same rules: signed out, it gets 401 like anything else.
//
//go:embed settings.html
var settingsPage []byte

// SettingsPage serves it.
func SettingsPage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(settingsPage)
}
