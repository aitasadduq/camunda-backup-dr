package utils

import (
	"fmt"
	"net/url"
)

// RedactURL reduces a URL to scheme and host for logging. Paths and query
// strings routinely carry tokens, and userinfo carries credentials outright.
func RedactURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "<invalid-url>"
	}
	return fmt.Sprintf("%s://%s", u.Scheme, u.Host)
}
