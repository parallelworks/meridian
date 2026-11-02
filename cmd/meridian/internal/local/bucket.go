package local

import (
	"strings"

	"github.com/parallelworks/meridian/problems"
)

// pwPrefix starts a platform's URI of one of its resources, as of a bucket:
// pw://user/name.
const pwPrefix = "pw://"

// resolveBucket gives the value of a bucket input, whose definition is
// def, as format 1 resolves one, as far as a run here can: a bucket's
// pw://user/name URI becomes its details (bucketValue), and with multi
// true, each URI of a list does. "" is no bucket, and is kept. A called
// workflow's input, called, also takes a bucket's details, a mapping, as
// its caller passes those it has, which are kept as they are, whose
// credentials, if any, no run here could have made; a run's own takes only
// the URI, as a platform's form gives it. What isn't a URI of that form,
// as a name alone or pw://name, which a platform reads as one of the
// user's own buckets, and a run here has no platform user to read it as,
// a mapping at a run's own input, and an item of a list that is neither,
// is problems.BucketURIInvalid. A URI of an input whose
// generateCredentials asks for the bucket's access keys, which only the
// platform that holds the bucket can make, is
// problems.BucketCredentialsNeedPlatform. A value of the wrong shape for
// the input is problems.InputNotText, or, with multi, problems.InputNotList,
// as the bug fix to format 1 refuses it (resolveEach).
func resolveBucket(def map[string]any, value any, called bool) (any, error) {
	return resolveEach(value, def["multi"] == true, func(v any) (any, error) {
		switch v := v.(type) {
		case string:
			return bucketValue(def, v)
		case map[string]any:
			if called {
				return v, nil
			}
		}
		return nil, problems.BucketURIInvalid.InputProblem(problems.Params{"value": problems.Show(v)})
	})
}

// bucketValue returns the details of the bucket uri names, a URI of the
// form pw://user/name, as format 1 gives a bucket's: its name, the user
// who owns it, and the URI, by which the platform's own CLI reaches it, as
// pw buckets cp does. What only the platform's record of the bucket holds,
// its id, its cloud provider (csp), the bucket's own name there
// (bucketName) and its region, a run here has none of, and leaves out, as
// the workspace's details leave out the platform's. A step that gives the
// URI to the platform's CLI needs that CLI signed in to the platform as it
// runs.
func bucketValue(def map[string]any, uri string) (map[string]any, error) {
	rest, ok := strings.CutPrefix(uri, pwPrefix)
	user, name, two := strings.Cut(rest, "/")
	if !ok || !two || user == "" || name == "" || strings.Contains(name, "/") {
		return nil, problems.BucketURIInvalid.InputProblem(problems.Params{"value": problems.Show(uri)})
	}
	if generate := def["generateCredentials"]; generate == true || generate == "true" {
		return nil, problems.BucketCredentialsNeedPlatform.InputProblem(nil)
	}
	return map[string]any{"type": "bucket", "name": name, "user": user, "uri": pwPrefix + user + "/" + name}, nil
}
