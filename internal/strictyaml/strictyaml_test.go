package strictyaml

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type strictNested struct {
	Enabled bool `yaml:"enabled"`
}

type strictExample struct {
	Name   string                  `yaml:"name"`
	Nested strictNested            `yaml:"nested"`
	Items  []strictNested          `yaml:"items"`
	Map    map[string]strictNested `yaml:"map"`
	Free   map[string]any          `yaml:"free"`
}

func TestUnknownKeysReportsNestedPathAndLine(t *testing.T) {
	var node yaml.Node
	if err := yaml.Unmarshal([]byte("name: ok\nnested:\n  typo: true\n"), &node); err != nil {
		t.Fatal(err)
	}
	err := UnknownKeys(&node, strictExample{})
	if err == nil || !strings.Contains(err.Error(), `unknown field "typo" in nested`) || !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("error=%v", err)
	}
}

func TestUnknownKeysChecksStructSlicesAndMapsButAllowsFreeMaps(t *testing.T) {
	for _, raw := range []string{
		"items:\n  - typo: true\n",
		"map:\n  first:\n    typo: true\n",
	} {
		var node yaml.Node
		if err := yaml.Unmarshal([]byte(raw), &node); err != nil {
			t.Fatal(err)
		}
		if err := UnknownKeys(&node, strictExample{}); err == nil {
			t.Fatalf("unknown key accepted: %s", raw)
		}
	}
	var free yaml.Node
	if err := yaml.Unmarshal([]byte("free:\n  arbitrary:\n    nested: value\n"), &free); err != nil {
		t.Fatal(err)
	}
	if err := UnknownKeys(&free, strictExample{}); err != nil {
		t.Fatalf("free map rejected: %v", err)
	}
}
