package plan

import "embed"

// Schemas holds the JSON Schemas (2020-12) of each version's encoding, at
// schema/<version>/: plan.schema.json, a Plan as Encode writes it, whose
// definitions describe the package's other encoded types too, and the
// expressions and values they hold; readyjob.schema.json,
// record.schema.json, runvalues.schema.json and jobresult.schema.json, a
// ReadyJob, a Record, RunValues and a JobResult, which refer to them; and
// workerapi.schema.json, the worker API's bodies, which are versioned with
// the plan, one definition for each body by its type's name, which refers
// to them for what a body carries of a run. A platform that reads a plan,
// or writes what Meridian reads, can check it against them. A version's
// schemas refer to each other by their names, as they are in one
// directory, so they resolve wherever the directory is.
//
//go:embed schema
var Schemas embed.FS
