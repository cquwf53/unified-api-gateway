package server

import (
	"testing"

	"unified-api-gateway/internal/config"
)

func TestCredentialEnvironmentFormsResolveConsistently(t *testing.T) {
	t.Setenv("GATEWAY_CREDENTIAL_REGRESSION", "resolved-value")
	for _, credential := range []config.Credential{
		{Env: "GATEWAY_CREDENTIAL_REGRESSION"},
		{Type: " env ", Env: "GATEWAY_CREDENTIAL_REGRESSION"},
		{Type: "api_key", Ref: "$ENV:GATEWAY_CREDENTIAL_REGRESSION"},
		{Type: "env", Ref: "env:GATEWAY_CREDENTIAL_REGRESSION"},
		{Ref: "$ENV:GATEWAY_CREDENTIAL_REGRESSION"},
	} {
		if got := resolveCredential(credential); got != "resolved-value" {
			t.Errorf("credential type=%q env=%q ref=%q resolved incorrectly", credential.Type, credential.Env, credential.Ref)
		}
	}
	if got := resolveCredential(config.Credential{Ref: "runtime-literal", Env: "GATEWAY_CREDENTIAL_REGRESSION"}); got != "runtime-literal" {
		t.Fatal("runtime literal ref precedence unexpectedly changed")
	}
}
