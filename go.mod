module github.com/parallelworks/meridian

go 1.27.3

// Enforce FIPS 140-3 mode everywhere, including `go test`: a non-approved
// algorithm fails at runtime instead of shipping.
godebug fips140=only

require (
	github.com/parallelworks/foundation v0.14.1
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3
	go.yaml.in/yaml/v3 v3.0.5
	golang.org/x/sys v0.49.0
	golang.org/x/text v0.43.0
)
