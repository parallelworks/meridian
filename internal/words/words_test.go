package words_test

import (
	"slices"
	"testing"

	"github.com/parallelworks/meridian/internal/words"
)

func TestSplit(t *testing.T) {
	for name, want := range map[string][]string{
		"":                   nil,
		"password":           {"password"},
		"apiKey":             {"api", "key"},
		"API_KEY":            {"api", "key"},
		"APIKey":             {"api", "key"},
		"api-key.v2":         {"api", "key", "v", "2"},
		"Client Secret":      {"client", "secret"},
		"aws/secret":         {"aws", "secret"},
		"OAuth2Token":        {"o", "auth", "2", "token"},
		"password2":          {"password", "2"},
		"__x__":              {"x"},
		"ÜberToken":          {"über", "token"},
		"$meta":              {"meta"},
		"HTTPServerURL":      {"http", "server", "url"},
		"s3BucketName":       {"s", "3", "bucket", "name"},
		"token-key":          {"token", "key"},
		"privateKey":         {"private", "key"},
		"x509CertificatePEM": {"x", "509", "certificate", "pem"},
	} {
		if got := words.Split(name); !slices.Equal(got, want) {
			t.Errorf("Split(%q) = %q, want %q", name, got, want)
		}
	}
}
