package local

import "github.com/parallelworks/meridian/problems"

// passedThrough are the input types whose value is a name, which a run
// reads as it was given, as format 1 gives it (passThrough): a scheduler's
// partition, account, QoS or queue on the cluster the input's resource:
// names; a Kubernetes cluster that a platform knows, or one of its
// namespaces or volume claims; or one of an organization's groups. A
// platform's form lists the names to choose from, but format 1 checks none
// of them as it creates a run: the scheduler, or the command the workflow
// gives the name to, checks it as the run uses it. A step that gives a
// Kubernetes cluster's name to pw kube auth needs the platform's own CLI,
// signed in, as it runs, here as on the platform: Meridian gives the name,
// as format 1 does, and nothing more.
var passedThrough = []string{
	"slurm-partitions", "slurm-accounts", "slurm-qos", "pbs-queues",
	"kubernetes-clusters", "kubernetes-namespaces", "kubernetes-pvc",
	"organization-groups",
}

// passThrough gives value, of an input of one of the passedThrough types,
// as it is: text, "" among it, or a mapping, as a run passes the value it
// has to a workflow it calls. Any other value is problems.InputNotText, as
// format 1 refuses it.
func passThrough(value any) (any, error) {
	switch value.(type) {
	case string, map[string]any:
		return value, nil
	}
	return nil, problems.InputNotText.InputProblem(problems.Params{"value": problems.Show(value)})
}
