package config

import (
	"strings"
	"testing"
)

func TestValidatePersistableRejectsLiteralSecrets(t *testing.T) {
	cfg := NewDefault()
	cfg.Credentials["upstream"] = Credential{Value: "literal"}
	cfg.Clients["client"] = ClientCredential{Key: "literal"}
	err := cfg.ValidatePersistable()
	if err == nil || !strings.Contains(err.Error(), "credential \"upstream\"") || !strings.Contains(err.Error(), "client \"client\"") {
		t.Fatalf("error=%v", err)
	}
}

func TestValidatePersistableAcceptsEnvironmentReferences(t *testing.T) {
	cfg := NewDefault()
	cfg.Credentials["upstream"] = Credential{Type: "env", Env: "UPSTREAM_KEY"}
	cfg.Clients["client"] = ClientCredential{KeyEnv: "CLIENT_KEY", KeyRef: "env:CLIENT_KEY"}
	if err := cfg.ValidatePersistable(); err != nil {
		t.Fatal(err)
	}
}
