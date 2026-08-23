// Package strictyaml validates that a decoded YAML mapping node only
// contains keys that exist on a target struct (matched by yaml tag). It is
// the reflection-driven replacement for hand-maintained key whitelists: new
// struct fields are picked up automatically, and error messages keep the
// original line numbers because the original node is inspected directly.
package strictyaml

import (
	"fmt"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

// UnknownKeys walks node against the struct type of exemplar and returns an
// error for every mapping key without a matching yaml-tagged field. Maps with
// non-struct values (such as map[string]any) accept any keys.
func UnknownKeys(node *yaml.Node, exemplar any) error {
	return checkValue(node, reflect.TypeOf(exemplar), "")
}

func checkValue(node *yaml.Node, t reflect.Type, path string) error {
	if node == nil || t == nil {
		return nil
	}
	switch t.Kind() {
	case reflect.Pointer:
		return checkValue(node, t.Elem(), path)
	case reflect.Slice, reflect.Array:
		elem := deref(t.Elem())
		if elem.Kind() != reflect.Struct || node.Kind != yaml.SequenceNode {
			return nil
		}
		for _, element := range node.Content {
			if err := checkValue(element, elem, path); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		valueType := deref(t.Elem())
		if valueType.Kind() != reflect.Struct || node.Kind != yaml.MappingNode {
			return nil
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			if err := checkValue(node.Content[i+1], valueType, path); err != nil {
				return err
			}
		}
		return nil
	case reflect.Struct:
		switch node.Kind {
		case yaml.DocumentNode:
			if len(node.Content) == 1 {
				return checkValue(node.Content[0], t, path)
			}
			return nil
		case yaml.MappingNode:
			known := structKeys(t)
			for i := 0; i+1 < len(node.Content); i += 2 {
				keyNode, valueNode := node.Content[i], node.Content[i+1]
				field, ok := known[keyNode.Value]
				if !ok {
					return fmt.Errorf("unknown field %q%s at line %d", keyNode.Value, pathSuffix(path), keyNode.Line)
				}
				if err := checkValue(valueNode, field, joinPath(path, keyNode.Value)); err != nil {
					return err
				}
			}
			return nil
		default:
			// Sequences bound to struct fields (TransformSpec's list form)
			// are validated by their own unmarshaller.
			return nil
		}
	default:
		return nil
	}
}

func deref(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// structKeys maps yaml tag names to the field's type so nested structs can be
// validated recursively. Anonymous (inline) fields are flattened.
func structKeys(structType reflect.Type) map[string]reflect.Type {
	known := map[string]reflect.Type{}
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		name := strings.Split(field.Tag.Get("yaml"), ",")[0]
		if name == "-" {
			continue
		}
		if field.Anonymous && name == "" {
			for innerName, innerType := range structKeys(field.Type) {
				known[innerName] = innerType
			}
			continue
		}
		if name == "" {
			name = strings.ToLower(field.Name)
		}
		known[name] = field.Type
	}
	return known
}

func joinPath(prefix, segment string) string {
	if prefix == "" {
		return segment
	}
	return prefix + "." + segment
}

func pathSuffix(path string) string {
	if path == "" {
		return ""
	}
	return " in " + path
}
