package plan

import "embed"

// Schemas holds the JSON Schemas (2020-12) of each version's encoding, at
// schema/<version>/: plan.schema.json, a Plan as Encode writes it, whose
// definitions describe the package's other encoded types too, and the
// expressions and values they hold; readyjob.schema.json,
// record.schema.json, runvalues.schema.json and jobresult.schema.json, a
// ReadyJob, a Record, RunValues and a JobResult, which refer to them; and
// workerapi.schema.json, the worker API's bodies for runs of the version,
// one definition for each body by its type's name, which refers to them
// for what a body carries of a run; and startstate.schema.json, what run
// state holds of Start's result, as compile.EncodeStartState writes it. A
// platform that reads a plan, or writes what Meridian reads, can check it
// against them. A version's schemas refer to each other by their names, as
// they are in one directory, so they resolve wherever the directory is.
//
// A version's schemas are frozen with it, but workerapi.schema.json: the
// worker API has a version of its own (workerapi.Version), and the bodies'
// schema of each plan version describes the newest API version's bodies,
// saying of a member a later API version adds which version that is.
//
//go:embed schema
var Schemas embed.FS
