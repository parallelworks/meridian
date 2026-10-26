package plan

import "embed"

// Schemas holds the JSON Schemas (2020-12) of each version's encoding, at
// schema/<version>/: plan.schema.json, a Plan as Encode writes it, whose
// definitions describe the package's other encoded types too, and the
// expressions and values they hold; and readyjob.schema.json,
// record.schema.json, runvalues.schema.json and jobresult.schema.json, a
// ReadyJob, a Record, RunValues and a JobResult, which refer to them. A
// platform that reads a plan, or writes what Meridian reads, can check it
// against them. The schemas refer to each other by their paths relative
// to each other, as the worker API's refer to them (workerapi.Schemas).
//
//go:embed schema
var Schemas embed.FS
