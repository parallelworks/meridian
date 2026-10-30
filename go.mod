module github.com/parallelworks/meridian

go 1.27.3

// Enforce FIPS 140-3 mode everywhere, including `go test`: a non-approved
// algorithm fails at runtime instead of shipping.
godebug fips140=only

require (
	github.com/parallelworks/foundation v0.15.0
	github.com/parallelworks/hopper v0.8.0
	github.com/parallelworks/hopper/driver/hoppersqlite v0.8.0
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.3
	go.yaml.in/yaml/v3 v3.0.5
	golang.org/x/crypto v0.58.0
	golang.org/x/sys v0.49.0
	golang.org/x/text v0.43.0
	modernc.org/sqlite v1.61.0
)

require (
	github.com/dustin/go-humanize v1.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.1.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	modernc.org/libc v1.79.0 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.12.1 // indirect
)
