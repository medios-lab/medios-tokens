package balancesync

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/medios-lab/medios-tokens/internal/ledger"
)

const token = "tok-71c2-do-not-print"

const meDocument = `{
  "org": {
    "name": "example",
    "caps": {"org": {"suggests_per_day": 1000}},
    "usage": {"suggests_24h": 987},
    "remaining": 13,
    "label": "thirteen",
    "debt": -4
  }
}`

func serve(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Api-Key"); got != "Key "+token {
			t.Errorf("credential header = %q", got)
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newPoller(t *testing.T, url string, mutate func(*Config)) *Poller {
	t.Helper()
	cfg := Config{
		URL:        url + "/v1/me?key=" + token,
		AuthHeader: "X-Api-Key",
		AuthValue:  "Key " + token,
		Timeout:    2 * time.Second,
		CapPath:    "org.caps.org.suggests_per_day",
		UsedPath:   "org.usage.suggests_24h",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func TestFetchReadsBalanceFromDotPaths(t *testing.T) {
	srv := serve(t, http.StatusOK, meDocument)
	cases := map[string]struct {
		mutate func(*Config)
		want   ledger.External
	}{
		"cap and used": {
			want: ledger.External{Available: 13, Cap: 1000, HasCap: true},
		},
		"remaining only": {
			mutate: func(c *Config) { c.CapPath, c.UsedPath, c.RemainingPath = "", "", "org.remaining" },
			want:   ledger.External{Available: 13},
		},
		"remaining with cap": {
			mutate: func(c *Config) { c.UsedPath, c.RemainingPath = "", "org.remaining" },
			want:   ledger.External{Available: 13, Cap: 1000, HasCap: true},
		},
		"used above cap floors at zero": {
			mutate: func(c *Config) { c.CapPath = "org.remaining" },
			want:   ledger.External{Available: 0, Cap: 13, HasCap: true},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := newPoller(t, srv.URL, tc.mutate).Fetch(context.Background())
			if err != nil || got != tc.want {
				t.Fatalf("Fetch = (%+v, %v), want %+v", got, err, tc.want)
			}
		})
	}
}

func TestFetchErrorsNeverCarryCredentialOrBody(t *testing.T) {
	// Each response body repeats the credential, as an endpoint that echoes
	// request details in its error page would.
	cases := map[string]struct {
		status int
		body   string
		mutate func(*Config)
		want   string
	}{
		"server error":   {status: http.StatusInternalServerError, body: "oops " + token, want: "HTTP 500"},
		"unauthorized":   {status: http.StatusUnauthorized, body: "bad key " + token, want: "HTTP 401"},
		"not json":       {status: http.StatusOK, body: "<html>" + token, want: "body is not JSON"},
		"missing key":    {status: http.StatusOK, body: `{"org":{"note":"` + token + `"}}`, want: `key "caps" is missing`},
		"not an object":  {status: http.StatusOK, body: meDocument, mutate: func(c *Config) { c.UsedPath = "org.name.first" }, want: "not inside a JSON object"},
		"not a number":   {status: http.StatusOK, body: meDocument, mutate: func(c *Config) { c.UsedPath = "org.label" }, want: "not a JSON number"},
		"negative value": {status: http.StatusOK, body: meDocument, mutate: func(c *Config) { c.UsedPath = "org.debt" }, want: "is negative"},
		"oversized body": {status: http.StatusOK, body: `{"pad":"` + strings.Repeat("x", maxResponseBytes) + `"}`, want: "body exceeds"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			srv := serve(t, tc.status, tc.body)
			_, err := newPoller(t, srv.URL, tc.mutate).Fetch(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Fetch error = %v, want it to contain %q", err, tc.want)
			}
			if strings.Contains(err.Error(), token) {
				t.Fatalf("error contains the credential: %v", err)
			}
		})
	}

	t.Run("connection refused", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		p := newPoller(t, srv.URL, nil)
		srv.Close()
		_, err := p.Fetch(context.Background())
		if err == nil || strings.Contains(err.Error(), token) {
			t.Fatalf("Fetch error = %v, want an error without the credential", err)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-release }))
		defer srv.Close()
		defer close(release)
		p := newPoller(t, srv.URL, func(c *Config) { c.Timeout = 50 * time.Millisecond })
		_, err := p.Fetch(context.Background())
		if err == nil || strings.Contains(err.Error(), token) {
			t.Fatalf("Fetch error = %v, want a timeout without the credential", err)
		}
	})
}

func TestFetchDoesNotFollowRedirects(t *testing.T) {
	var followed atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed.Store(true)
		_, _ = io.WriteString(w, meDocument)
	}))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusFound)
	}))
	defer srv.Close()

	_, err := newPoller(t, srv.URL, nil).Fetch(context.Background())
	if err == nil || !strings.Contains(err.Error(), "HTTP 302") {
		t.Fatalf("Fetch error = %v, want HTTP 302", err)
	}
	if followed.Load() {
		t.Fatal("the redirect was followed")
	}
}

func TestEndpointOmitsQueryAndUserInfo(t *testing.T) {
	p, err := New(Config{
		URL:           "https://user:" + token + "@api.example.com/v1/me?key=" + token,
		Timeout:       time.Second,
		RemainingPath: "remaining",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := p.Endpoint(); got != "https://api.example.com/v1/me" {
		t.Fatalf("Endpoint = %q", got)
	}
}

func TestNewValidatesConfig(t *testing.T) {
	valid := Config{URL: "https://api.example.com/v1/me", Timeout: time.Second, CapPath: "cap", UsedPath: "used"}
	cases := map[string]func(*Config){
		"relative url":              func(c *Config) { c.URL = "/v1/me" },
		"unsupported scheme":        func(c *Config) { c.URL = "file:///etc/quota.json" },
		"no paths":                  func(c *Config) { c.CapPath, c.UsedPath = "", "" },
		"used without cap":          func(c *Config) { c.CapPath = "" },
		"cap without used":          func(c *Config) { c.UsedPath = "" },
		"used and remaining":        func(c *Config) { c.RemainingPath = "remaining" },
		"header without value":      func(c *Config) { c.AuthHeader = "Authorization" },
		"value without header":      func(c *Config) { c.AuthValue = "Bearer x" },
		"zero timeout":              func(c *Config) { c.Timeout = 0 },
		"bad url with a credential": func(c *Config) { c.URL = "://" + token },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := valid
			mutate(&cfg)
			_, err := New(cfg)
			if err == nil {
				t.Fatal("invalid config was accepted")
			}
			if strings.Contains(err.Error(), token) {
				t.Fatalf("error contains the credential: %v", err)
			}
		})
	}
	if _, err := New(valid); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
