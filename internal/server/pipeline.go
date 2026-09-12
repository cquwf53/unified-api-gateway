package server

import (
	"fmt"
	"strings"

	"unified-api-gateway/internal/config"
	"unified-api-gateway/internal/transform"
)

// compiledPipeline holds the pre-compiled request/response transform chains
// of one profile. Compiling once per activation removes the per-request
// validate-and-clone cost of one-shot transform.Apply calls.
type compiledPipeline struct {
	request  []*transform.CompiledTransform
	response []*transform.CompiledTransform
}

// resolveSpecs recursively expands named references before compilation. A
// missing reference or cycle is an activation error: silently retaining a
// named spec would turn a security-sensitive rule into a no-op.
func resolveSpecs(cfg *config.Config, specs []transform.TransformSpec) ([]transform.TransformSpec, error) {
	if cfg == nil {
		return nil, fmt.Errorf("transform: config is nil")
	}
	var resolve func(transform.TransformSpec, []string) (transform.TransformSpec, error)
	resolve = func(spec transform.TransformSpec, stack []string) (transform.TransformSpec, error) {
		name := strings.TrimSpace(spec.Name)
		if name == "" {
			for i := range spec.Steps {
				step, err := resolve(spec.Steps[i], stack)
				if err != nil {
					return transform.TransformSpec{}, err
				}
				spec.Steps[i] = step
			}
			return spec, nil
		}
		for _, previous := range stack {
			if previous == name {
				return transform.TransformSpec{}, fmt.Errorf("transform reference cycle: %s -> %s", strings.Join(append(stack, name), " -> "), name)
			}
		}
		reusable, ok := cfg.Transforms[name]
		if !ok {
			return transform.TransformSpec{}, fmt.Errorf("named transform %q not found", name)
		}
		return resolve(transform.CloneSpec(reusable), append(stack, name))
	}
	out := make([]transform.TransformSpec, 0, len(specs))
	for _, spec := range specs {
		resolved, err := resolve(transform.CloneSpec(spec), nil)
		if err != nil {
			return nil, err
		}
		out = append(out, resolved)
	}
	return out, nil
}

func compileSteps(specs []transform.TransformSpec) ([]*transform.CompiledTransform, error) {
	out := make([]*transform.CompiledTransform, 0, len(specs))
	for index, spec := range specs {
		compiled, err := transform.Compile(spec)
		if err != nil {
			return nil, fmt.Errorf("transform step %d (%q): %w", index, spec.Name, err)
		}
		out = append(out, compiled)
	}
	return out, nil
}

func buildPipelines(cfg *config.Config) (map[string]*compiledPipeline, error) {
	if cfg == nil {
		return map[string]*compiledPipeline{}, nil
	}
	pipelines := make(map[string]*compiledPipeline, len(cfg.Profiles))
	for name, profile := range cfg.Profiles {
		requestSpecs, err := resolveSpecs(cfg, profile.AllRequestTransforms())
		if err != nil {
			return nil, fmt.Errorf("profile %q request transforms: %w", name, err)
		}
		responseSpecs, err := resolveSpecs(cfg, profile.AllResponseTransforms())
		if err != nil {
			return nil, fmt.Errorf("profile %q response transforms: %w", name, err)
		}
		request, err := compileSteps(requestSpecs)
		if err != nil {
			return nil, fmt.Errorf("profile %q request transforms: %w", name, err)
		}
		response, err := compileSteps(responseSpecs)
		if err != nil {
			return nil, fmt.Errorf("profile %q response transforms: %w", name, err)
		}
		pipelines[name] = &compiledPipeline{request: request, response: response}
	}
	return pipelines, nil
}

// compiledRequestSteps returns the cached request chain for a profile,
// falling back to strict one-shot compilation for preview callers that use a
// profile assembled outside the published snapshot.
func (s *Server) compiledRequestSteps(pinned *gatewayState, cfg *config.Config, profile config.Profile) []*transform.CompiledTransform {
	if pinned != nil {
		if pipeline, ok := pinned.pipelines[profileRuntimeID(profile)]; ok {
			return pipeline.request
		}
	}
	resolved, err := resolveSpecs(cfg, profile.AllRequestTransforms())
	if err != nil {
		return nil
	}
	compiled, err := compileSteps(resolved)
	if err != nil {
		return nil
	}
	return compiled
}

func (s *Server) compiledResponseSteps(pinned *gatewayState, cfg *config.Config, profile config.Profile) []*transform.CompiledTransform {
	if pinned != nil {
		if pipeline, ok := pinned.pipelines[profileRuntimeID(profile)]; ok {
			return pipeline.response
		}
	}
	resolved, err := resolveSpecs(cfg, profile.AllResponseTransforms())
	if err != nil {
		return nil
	}
	compiled, err := compileSteps(resolved)
	if err != nil {
		return nil
	}
	return compiled
}

func profileRuntimeID(profile config.Profile) string {
	if profile.RuntimeID != "" {
		return profile.RuntimeID
	}
	return profile.Name
}
