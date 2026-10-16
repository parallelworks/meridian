module github.com/parallelworks/meridian

go 1.27.2

// Enforce FIPS 140-3 mode everywhere, including `go test`: a non-approved
// algorithm fails at runtime instead of shipping.
godebug fips140=only
