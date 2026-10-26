package workerapi

import "embed"

// Schemas holds the JSON Schema (2020-12) of the API's bodies for each
// plan version, at schema/<version>/workerapi.schema.json: one definition
// for each body, by its type's name. What a body carries of a run, its
// values (plan.RunValues), a ready job (plan.ReadyJob) and job results
// (plan.JobResult), they refer to the plan's schemas for
// (plan.Schemas), by their paths in the repository relative to these.
//
//go:embed schema
var Schemas embed.FS
