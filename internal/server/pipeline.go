package server

import (
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

// resolveSpecs replaces named references with the reusable transforms from
// the configuration before compilation.
func resolveSpecs(cfg *config.Config, specs []transform.TransformSpec) []transform.TransformSpec {
	out := make([]transform.TransformSpec, 0, len(specs))
	for _, spec := range specs {
		if spec.Name != "" {
			if reusable, exists := cfg.Transforms[spec.Name]; exists {
				spec = reusable
			}
		}
		out = append(out, spec)
	}
	return out
}

func compileSteps(specs []transform.TransformSpec) []*transform.CompiledTransform {
	out := make([]*transform.CompiledTransform, 0, len(specs))
	for _, spec := range specs {
		if compiled, err := transform.Compile(spec); err == nil {
			out = append(out, compiled)
		}
	}
	return out
}

func buildPipelines(cfg *config.Config) map[string]*compiledPipeline {
	if cfg == nil {
		return map[string]*compiledPipeline{}
	}
	pipelines := make(map[string]*compiledPipeline, len(cfg.Profiles))
	for name, profile := range cfg.Profiles {
		pipelines[name] = &compiledPipeline{
			request:  compileSteps(resolveSpecs(cfg, profile.AllRequestTransforms())),
			response: compileSteps(resolveSpecs(cfg, profile.AllResponseTransforms())),
		}
	}
	return pipelines
}

// compiledRequestSteps returns the cached request chain for a profile,
// falling back to a one-shot compilation for profiles assembled outside a
// configuration snapshot (for example the preview endpoint).
func (s *Server) compiledRequestSteps(cfg *config.Config, profile config.Profile) []*transform.CompiledTransform {
	if state := s.state.Load(); state != nil {
		if pipeline, ok := state.pipelines[profile.Name]; ok {
			return pipeline.request
		}
	}
	return compileSteps(resolveSpecs(cfg, profile.AllRequestTransforms()))
}

func (s *Server) compiledResponseSteps(cfg *config.Config, profile config.Profile) []*transform.CompiledTransform {
	if state := s.state.Load(); state != nil {
		if pipeline, ok := state.pipelines[profile.Name]; ok {
			return pipeline.response
		}
	}
	return compileSteps(resolveSpecs(cfg, profile.AllResponseTransforms()))
}
