package plan_test

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/plan"
)

// since is the plan version that gave each member of the package's
// encoded types, by its type and JSON name: a Plan, a ReadyJob, a secret
// Record, RunValues and a JobResult, and the types they hold. Version 1 is
// frozen, so a member added later is in a later version, which raises
// plan.Version, as the worker API's since table does for its bodies'
// own types, with the API's version.
var since = map[string]int{
	"Plan.version": 1, "Plan.run": 1, "Plan.jobs": 1, "Plan.warnings": 1, "Plan.calls": 2, "Plan.checkouts": 2,
	"Call.caller": 2, "Call.step": 2, "Call.source": 2,
	"CheckoutPin.caller": 2, "CheckoutPin.step": 2, "CheckoutPin.checkout": 2,
	"Checkout.repo": 2, "Checkout.revision": 2, "Checkout.commit": 2,

	"Run.source": 1, "Run.format": 1, "Run.release": 1, "Run.name": 1, "Run.description": 1, "Run.icon": 1,
	"Run.thumbnail": 1, "Run.inputs": 1, "Run.definitions": 1, "Run.withheld": 1, "Run.decisions": 1, "Run.env": 1,
	"Run.timeout": 1, "Run.remote": 1, "Run.sessions": 1, "Run.sessionNames": 1, "Run.secrets": 1, "Run.derived": 1,
	"Run.private": 1, "Run.variables": 1, "Run.permissions": 1, "Run.limits": 1, "Run.parent": 2,
	"Run.treeVariables": 2,
	"ParentRun.run":     2, "ParentRun.job": 2, "ParentRun.step": 2, "ParentRun.try": 2, "ParentRun.root": 2,
	"ParentRun.depth": 2, "ParentRun.env": 2, "ParentRun.private": 2, "ParentRun.secrets": 2,
	"Limits.maxInstances": 1,
	"Source.location":     1, "Source.revision": 1, "Source.digest": 1, "Source.ref": 1, "Source.remote": 2,
	"SourceRef.uses": 1, "SourceRef.path": 1, "SourceRef.host": 1,

	"Job.key": 1, "Job.name": 1, "Job.strategy": 1, "Job.needs": 1, "Job.needsExpr": 1, "Job.if": 1,
	"Job.runsOn": 1, "Job.resources": 1, "Job.ssh": 1, "Job.retry": 1, "Job.timeout": 1, "Job.env": 1,
	"Job.workingDirectory": 1, "Job.steps": 1, "Job.outputs": 1, "Job.path": 1,
	"Need.job": 1, "Need.any": 1,
	"Strategy.matrix": 1, "Strategy.failFast": 1, "Strategy.maxParallel": 1,
	"JobRetry.reasons": 1, "JobRetry.maxRetries": 1, "JobRetry.interval": 1,
	"Step.id": 1, "Step.name": 1, "Step.if": 1, "Step.run": 1, "Step.uses": 1, "Step.source": 1, "Step.checkout": 2, "Step.with": 1,
	"Step.env": 1, "Step.shell": 1, "Step.workingDirectory": 1, "Step.ssh": 1, "Step.retry": 1, "Step.timeout": 1,
	"Step.ignoreErrors": 1, "Step.earlyCancel": 1, "Step.cleanup": 1, "Step.path": 1,
	"StepRetry.maxRetries": 1, "StepRetry.interval": 1, "StepRetry.timeout": 1, "StepRetry.cleanOnRetry": 1,

	"ReadyJob.key": 1, "ReadyJob.name": 1, "ReadyJob.matrix": 1, "ReadyJob.target": 1, "ReadyJob.resources": 1,
	"ReadyJob.retry": 1, "ReadyJob.timeoutSeconds": 1, "ReadyJob.workingDirectory": 1, "ReadyJob.ifAlways": 1,
	"ReadyJob.env": 1, "ReadyJob.steps": 1, "ReadyJob.outputs": 1, "ReadyJob.private": 1, "ReadyJob.secrets": 1,
	"ReadyJob.privateFields": 1, "ReadyJob.secretFields": 1, "ReadyJob.masked": 1, "ReadyJob.path": 1,
	"Target.group": 1, "Target.environment": 1, "Target.computeTarget": 1, "Target.inline": 1,
	"Environment.cluster": 1, "Environment.name": 1, "Environment.params": 1,
	"ComputeTarget.mode": 1, "ComputeTarget.targetId": 1, "ComputeTarget.environmentId": 1,
	"ComputeTarget.schedulingParams": 1, "ComputeTarget.newWorker": 1, "ComputeTarget.workerId": 1,
	"Inline.name": 1, "Inline.backend": 1, "Inline.isolation": 1, "Inline.ssh": 1,
	"Resources.milliCPU": 1, "Resources.memory": 1, "Resources.gpu": 1,
	"ReadyJobRetry.reasons": 1, "ReadyJobRetry.maxRetries": 1, "ReadyJobRetry.intervalSeconds": 1,

	"Record.version": 1, "Record.planDigest": 1, "Record.submitted": 1, "Record.withheld": 1, "Record.decisions": 1,
	"Record.means": 1, "Record.run": 1, "Record.timeoutSeconds": 1, "Record.expansions": 1, "Record.jobs": 1,
	"JobRecord.outputs": 1, "JobRecord.steps": 1, "Record.inherited": 2,
	"Inherited.env": 2, "Inherited.masked": 2,
	"Record.stepRun": 2, "StepRun.ssh": 2, "StepRun.private": 2, "StepRun.state": 2, "StepRun.masked": 2,
	"Expansion.instances": 1, "Expansion.failFast": 1, "Expansion.maxParallel": 1,
	"Instance.key": 1, "Instance.index": 1, "Instance.matrix": 1, "Instance.private": 1, "Instance.secret": 1,

	"RunValues.format": 1, "RunValues.inputs": 1, "RunValues.env": 1, "RunValues.org": 1, "RunValues.vars": 1,
	"RunValues.remote": 1, "RunValues.sessions": 1, "RunValues.private": 1, "RunValues.secrets": 1,
	"RunValues.derived": 1,
	"JobResult.outcome": 1, "JobResult.outputs": 1, "JobResult.private": 1, "JobResult.secrets": 1,
	"JobResult.derived": 1, "JobResult.steps": 1, "JobResult.secretSteps": 1, "JobResult.derivedSteps": 1,
	"JobResult.needsFailed": 1, "JobResult.cancelled": 1,
}

// TestMembersAreVersioned checks the rule docs/design.md gives for a
// change to the plan's encoding, under Plans across versions: every member
// of every encoded type has the version that gave it, none later than
// plan.Version; and a member added after version 1 is left out when it is
// empty, so that a plan of an earlier version, upgraded as it decodes,
// encodes as it did; but for the members of a type a later version added,
// which are written where the member that holds it is, and are of its
// version, or later. A member added without an entry fails it, as does an
// entry for a member that is gone.
func TestMembersAreVersioned(t *testing.T) {
	seen := map[string]bool{}
	// holder is the version of the member that holds typ: 1 for a root.
	var walk func(typ reflect.Type, holder int)
	walk = func(typ reflect.Type, holder int) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || slices.Contains(opaqueTypes, typ) || seen[typ.Name()] {
			return
		}
		seen[typ.Name()] = true
		for f := range typ.Fields() {
			name := jsonName(f)
			if name == "" {
				continue
			}
			_, options, _ := strings.Cut(f.Tag.Get("json"), ",")
			member := typ.Name() + "." + name
			version, ok := since[member]
			switch {
			case !ok:
				t.Errorf("%s has no version: give it the next plan version, and raise plan.Version", member)
			case version > plan.Version:
				t.Errorf("%s is from version %d, after plan.Version %d", member, version, plan.Version)
			case version < holder:
				t.Errorf("%s is from version %d, before the member that holds it, of version %d", member, version, holder)
			case version > holder && !strings.Contains(options, "omitzero") && !strings.Contains(options, "omitempty"):
				t.Errorf("%s is from version %d, and isn't left out when empty", member, version)
			}
			seen[member] = true
			walk(f.Type, version)
		}
	}
	for _, root := range encodedRoots {
		walk(root, 1)
	}
	for member := range since {
		if !seen[member] {
			t.Errorf("%s is in no encoded type", member)
		}
	}
}
