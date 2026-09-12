package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
	"unified-api-gateway/internal/transform"
)

// MarshalYAML preserves numeric JSON scalars in programmatically supplied rule
// values. yaml.v3 otherwise encodes json.Number as a quoted string. Typed
// configuration fields, including integer status_map keys, keep their YAML form.
func (c *Config) MarshalYAML() (any, error) {
	if c == nil {
		return nil, nil
	}
	clone := c.Clone()
	normalize := func(values map[string]any) error {
		for key, value := range values {
			node, err := jsonYAMLNode(value)
			if err != nil {
				return err
			}
			values[key] = node
		}
		return nil
	}
	var walk func(transform.TransformSpec) error
	walk = func(spec transform.TransformSpec) error {
		if err := normalize(spec.Body.Set); err != nil {
			return err
		}
		if err := normalize(spec.Body.Default); err != nil {
			return err
		}
		for _, step := range spec.Steps {
			if err := walk(step); err != nil {
				return err
			}
		}
		return nil
	}
	for name, profile := range clone.Profiles {
		for _, spec := range append(profile.AllRequestTransforms(), profile.AllResponseTransforms()...) {
			if err := walk(spec); err != nil {
				return nil, fmt.Errorf("profile %q: %w", name, err)
			}
		}
	}
	for name, spec := range clone.Transforms {
		if err := walk(spec); err != nil {
			return nil, fmt.Errorf("transform %q: %w", name, err)
		}
	}
	if err := normalize(clone.Policies); err != nil {
		return nil, fmt.Errorf("policies: %w", err)
	}
	type plainConfig Config
	return (*plainConfig)(clone), nil
}

func jsonYAMLNode(value any) (*yaml.Node, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, err
	}
	return dynamicYAMLNode(decoded)
}

func dynamicYAMLNode(value any) (*yaml.Node, error) {
	switch value := value.(type) {
	case json.Number:
		tag := "!!int"
		if strings.ContainsAny(string(value), ".eE") {
			tag = "!!float"
		}
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: string(value)}, nil
	case map[string]any:
		node := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			child, err := dynamicYAMLNode(value[key])
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, child)
		}
		return node, nil
	case []any:
		node := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
		for _, item := range value {
			child, err := dynamicYAMLNode(item)
			if err != nil {
				return nil, err
			}
			node.Content = append(node.Content, child)
		}
		return node, nil
	default:
		node := &yaml.Node{}
		if err := node.Encode(value); err != nil {
			return nil, err
		}
		return node, nil
	}
}
