package conformancetest

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/parallelworks/meridian/compile"
	"github.com/parallelworks/meridian/conformance"
	"github.com/parallelworks/meridian/expr"
)

// CreateContexts gives a case's contexts as compile.Options.Contexts takes
// them when the run is created: as the case gives them, with each secret
// variable's value unknown.
func CreateContexts(c conformance.Case) map[string]any {
	out := maps.Clone(c.Contexts)
	if out == nil {
		out = map[string]any{}
	}
	if len(c.SecretVariables) > 0 {
		vars, _ := out["vars"].(map[string]any)
		vars = maps.Clone(vars)
		if vars == nil {
			vars = map[string]any{}
		}
		for _, name := range c.SecretVariables {
			vars[name] = expr.Unknown{Name: "vars"}
		}
		out["vars"] = vars
	}
	return out
}

// StartVariables gives a case's variables as the run reads them as it
// starts: its contexts' org and vars, with those its start-contexts
// replace, each secret one secret.
func StartVariables(c conformance.Case) compile.Variables {
	out := Variables{}
	for _, context := range []string{"org", "vars"} {
		vars, _ := c.Contexts[context].(map[string]any)
		if v, ok := c.StartContexts[context].(map[string]any); ok {
			vars = v
		}
		for name, v := range vars {
			if s, ok := v.(string); ok {
				out[context+"."+name] = compile.Variable{Value: s, Secret: context == "vars" && slices.Contains(c.SecretVariables, name)}
			}
		}
	}
	return out
}

// Variables are variables a test gives Create and Start, by their names as
// the Variables interface reads them: a context, a dot and the name as it
// is, as in vars.TOKEN or vars.my.token.
type Variables map[string]compile.Variable

// Contexts gives the variables as compile.Options.Contexts takes them when
// the run is created: each plain one's value, and each secret one unknown.
func (v Variables) Contexts() map[string]any {
	out := map[string]any{"org": map[string]any{}, "vars": map[string]any{}}
	for k, x := range v {
		context, name, _ := strings.Cut(k, ".")
		var value any = x.Value
		if x.Secret {
			value = expr.Unknown{Name: context}
		}
		out[context].(map[string]any)[name] = value
	}
	return out
}

// Read gives the variables names name, as compile.Variables says.
func (v Variables) Read(_ context.Context, names []string, secrets bool) (map[string]compile.Variable, error) {
	out := map[string]compile.Variable{}
	for _, name := range names {
		_, _, one := strings.Cut(name, ".")
		for k, x := range v {
			if one && k != name || !one && !strings.HasPrefix(k, name+".") {
				continue
			}
			if x.Secret && !secrets {
				x.Value = ""
			}
			out[k] = x
		}
	}
	return out, nil
}
