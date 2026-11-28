package main

import (
	"strings"
	"testing"
	"time"

	"github.com/medios-lab/medios-tokens/internal/balancesync"
)

const testToken = "tok-55d0-do-not-print"

func envWith(vars map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		v, ok := vars[name]
		return v, ok
	}
}

const twoPoolConfig = `{
  "pools": [
    {
      "resource": "tauopt_suggestions",
      "initial": 1000,
      "cap": 1000,
      "sync": {
        "url": "https://api.tauopt.com/v1/me",
        "auth_header": "Authorization",
        "auth_prefix": "Bearer ",
        "auth_env": "TAUOPT_API_TOKEN",
        "interval": "60s",
        "cap_path": "org.caps.org.suggests_per_day",
        "used_path": "org.usage.suggests_24h"
      }
    },
    {
      "resource": "openrouter_usd",
      "initial": 50.0,
      "cap": 50.0,
      "refill_amount": 0,
      "provisioner": "openrouter"
    }
  ]
}`

func TestParsePoolConfigTwoPools(t *testing.T) {
	setup, err := parsePoolConfig([]byte(twoPoolConfig), envWith(map[string]string{"TAUOPT_API_TOKEN": testToken}))
	if err != nil {
		t.Fatalf("parsePoolConfig: %v", err)
	}
	if len(setup.specs) != 2 ||
		setup.specs[0].Resource != "tauopt_suggestions" || setup.specs[0].Initial != 1000 || setup.specs[0].Cap != 1000 ||
		setup.specs[1].Resource != "openrouter_usd" || setup.specs[1].Initial != 50 || setup.specs[1].Cap != 50 {
		t.Fatalf("specs = %+v", setup.specs)
	}
	if setup.provisionerResource != "openrouter_usd" {
		t.Fatalf("provisionerResource = %q", setup.provisionerResource)
	}
	tau := setup.options["tauopt_suggestions"]
	if tau.Source == nil || tau.SyncInterval != 60*time.Second {
		t.Fatalf("tauopt options = %+v", tau)
	}
	if usd := setup.options["openrouter_usd"]; usd.Source != nil || usd.RefillAmount != 0 {
		t.Fatalf("openrouter options = %+v", usd)
	}
	// The startup summary names the variable, not its value.
	joined := strings.Join(setup.summaries, "\n")
	if !strings.Contains(joined, "auth_env=TAUOPT_API_TOKEN") || !strings.Contains(joined, "sync=https://api.tauopt.com/v1/me") {
		t.Fatalf("summaries = %s", joined)
	}
	if strings.Contains(joined, testToken) {
		t.Fatalf("summary contains the credential: %s", joined)
	}
}

func TestParsePoolConfigSyncDefaults(t *testing.T) {
	cfg := `{"pools":[{"resource":"q","sync":{"url":"http://127.0.0.1:1/q","remaining_path":"left"}}]}`
	setup, err := parsePoolConfig([]byte(cfg), envWith(nil))
	if err != nil {
		t.Fatalf("parsePoolConfig: %v", err)
	}
	if got := setup.options["q"].SyncInterval; got != balancesync.DefaultInterval {
		t.Fatalf("default interval = %s, want %s", got, balancesync.DefaultInterval)
	}
	want := "timeout=" + balancesync.DefaultTimeout.String() + " auth=none"
	if !strings.Contains(setup.summaries[0], want) {
		t.Fatalf("summary = %q, want it to contain %q", setup.summaries[0], want)
	}
}

func TestParsePoolConfigRejects(t *testing.T) {
	sync := func(fields string) string {
		return `{"pools":[{"resource":"q","initial":1,"cap":1,"sync":{` + fields + `}}]}`
	}
	const paths = `"url":"https://api.example.com/q","cap_path":"c","used_path":"u"`
	cases := map[string]struct {
		cfg  string
		env  map[string]string
		want string
	}{
		"credential variable unset": {
			cfg:  sync(paths + `,"auth_header":"Authorization","auth_prefix":"Bearer ","auth_env":"Q_TOKEN"`),
			want: `pool "q": sync credential: environment variable Q_TOKEN is unset or empty`,
		},
		"credential variable empty": {
			cfg:  sync(paths + `,"auth_header":"Authorization","auth_env":"Q_TOKEN"`),
			env:  map[string]string{"Q_TOKEN": ""},
			want: "environment variable Q_TOKEN is unset or empty",
		},
		"header without variable": {
			cfg:  sync(paths + `,"auth_header":"Authorization"`),
			want: "without auth_env",
		},
		"prefix without variable": {
			cfg:  sync(paths + `,"auth_prefix":"Bearer "`),
			want: "without auth_env",
		},
		"variable without header": {
			cfg:  sync(paths + `,"auth_env":"Q_TOKEN"`),
			env:  map[string]string{"Q_TOKEN": testToken},
			want: "without auth_header",
		},
		"bad interval": {
			cfg:  sync(paths + `,"interval":"sixty"`),
			want: `sync interval "sixty" is not a positive duration`,
		},
		"zero timeout": {
			cfg:  sync(paths + `,"timeout":"0s"`),
			want: "not a positive duration",
		},
		"used without cap": {
			cfg:  sync(`"url":"https://api.example.com/q","used_path":"u"`),
			want: "cap_path with used_path",
		},
		"refill on synced pool": {
			cfg:  `{"pools":[{"resource":"q","refill_amount":5,"sync":{` + paths + `}}]}`,
			want: "refill_amount cannot be combined with sync",
		},
		"unknown field": {
			cfg:  `{"pools":[{"resource":"q","intial":5}]}`,
			want: `unknown field "intial"`,
		},
		"no pools": {
			cfg:  `{"pools":[]}`,
			want: "declares no pools",
		},
		"unnamed pool": {
			cfg:  `{"pools":[{"initial":5}]}`,
			want: "pools[0]: resource is required",
		},
		"duplicate pool": {
			cfg:  `{"pools":[{"resource":"q"},{"resource":"q"}]}`,
			want: `pool "q" is declared twice`,
		},
		"negative amount": {
			cfg:  `{"pools":[{"resource":"q","initial":-1}]}`,
			want: "must be non-negative",
		},
		"unknown provisioner": {
			cfg:  `{"pools":[{"resource":"q","provisioner":"acme"}]}`,
			want: `unknown provisioner "acme"`,
		},
		"two provisioner pools": {
			cfg:  `{"pools":[{"resource":"a","provisioner":"openrouter"},{"resource":"b","provisioner":"openrouter"}]}`,
			want: "both set provisioner",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := parsePoolConfig([]byte(tc.cfg), envWith(tc.env))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
			if strings.Contains(err.Error(), testToken) {
				t.Fatalf("error contains the credential: %v", err)
			}
		})
	}
}
