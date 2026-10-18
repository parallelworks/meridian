module github.com/parallelworks/meridian

go 1.27.2

// Enforce FIPS 140-3 mode everywhere, including `go test`: a non-approved
// algorithm fails at runtime instead of shipping.
godebug fips140=only

require (
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3
	go.yaml.in/yaml/v3 v3.0.5
	golang.org/x/text v0.43.0
)
