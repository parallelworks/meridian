package plan

import (
	"bytes"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/parallelworks/meridian/internal/words"
)

func naiveFind(s string, values []string) []span {
	var found []span
	for _, v := range values {
		if v == "" {
			continue
		}
		for i := 0; i+len(v) <= len(s); i++ {
			if s[i:i+len(v)] == v {
				found = append(found, span{i, i + len(v)})
			}
		}
	}
	slices.SortStableFunc(found, func(a, b span) int { return a.start - b.start })
	var out []span
	for _, f := range found {
		if n := len(out); n > 0 && f.start < out[n-1].end {
			out[n-1].end = max(out[n-1].end, f.end)
			continue
		}
		out = append(out, f)
	}
	return out
}

func TestMatcherAgainstNaive(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 5))
	alpha := []byte{'a', 'b', '\n', '"', 0xff, 0}
	word := func(max int) string {
		b := make([]byte, r.IntN(max+1))
		for i := range b {
			b[i] = alpha[r.IntN(len(alpha))]
		}
		return string(b)
	}
	for range 100000 {
		var values []string
		for range 1 + r.IntN(6) {
			values = append(values, word(5))
		}
		s := word(40)
		var nonEmpty []string
		for _, v := range values {
			if v != "" {
				nonEmpty = append(nonEmpty, v)
			}
		}
		got, _ := newMatcher(nonEmpty).find(s)
		want := naiveFind(s, values)
		if !slices.Equal(got, want) {
			t.Fatalf("values %q in %q: %v, want %v", values, s, got, want)
		}
	}
}

// naiveMask masks s with every shown form of added, found naively.
func naiveMask(s string, added []string, lines bool) string {
	var all []string
	for _, v := range added {
		all = append(all, shownAs(v, lines)...)
	}
	return masked(s, naiveFind(s, all))
}

// A Masker that values are added to between masks, so that it has several
// levels, masks as a naive search for every shown form does, and so does
// its Writer, however its text is split.
func TestMaskerLevelsAgainstNaive(t *testing.T) {
	r := rand.New(rand.NewPCG(21, 23))
	alpha := []string{"a", "b", "c", "\n", " ", "\"", "<", "&", "é", " ", "x9"}
	word := func(max int) string {
		var b strings.Builder
		for range r.IntN(max + 1) {
			b.WriteString(alpha[r.IntN(len(alpha))])
		}
		return b.String()
	}
	for range 5000 {
		lines := r.IntN(2) == 0
		m := &Masker{lines: lines}
		var added []string
		for range 1 + r.IntN(8) {
			v := word(12)
			m.Add(v)
			added = append(added, v)
			s := word(40)
			if got, want := m.Mask(s), naiveMask(s, added, lines); got != want {
				t.Fatalf("lines %v, added %q, %q: %q, want %q (levels %d)", lines, added, s, got, want, len(m.levels))
			}
		}
		s := word(60)
		var out bytes.Buffer
		w := m.Writer(&out)
		for rest := s; rest != ""; {
			n := min(len(rest), 1+r.IntN(7))
			if _, err := w.Write([]byte(rest[:n])); err != nil {
				t.Fatal(err)
			}
			rest = rest[n:]
		}
		_ = w.Close()
		if want := naiveMask(s, added, lines); out.String() != want {
			t.Fatalf("writer: added %q, %q: %q, want %q", added, s, out.String(), want)
		}
	}
}

// No shown form of a value is shorter than the value, or than minLine, but
// the value without the line breaks it ends with.
func TestShownFormsLength(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	alpha := []string{"a", "1", "\n", " ", "\"", "<", "&", "é", " ", "}", "\t"}
	for range 50000 {
		var b strings.Builder
		for range r.IntN(20) {
			b.WriteString(alpha[r.IntN(len(alpha))])
		}
		v := b.String()
		for _, f := range shownAs(v, true) {
			if f == "" {
				continue
			}
			if len(f) < min(len(v), minLine) && !slices.Contains([]string{strings.TrimRight(v, "\r\n")}, f) {
				t.Fatalf("%q shows as %q", v, f)
			}
		}
	}
}

// A key a credential is kept under is one of the credential words, has one
// among its words, or ends with key or keys after a qualifier, whatever
// its case and whatever separates its words, as a space or a slash does,
// and isn't one whose last word describes a credential rather than holding
// one. Of the keys of common configs, only those that hold a credential
// are.
func TestCredentialKey(t *testing.T) {
	for key, want := range map[string]bool{
		"password": true, "PASSWORD": true, "db_password": true, "api_key": true, "apiKey": true, "APIKey": true,
		"accessToken": true, "aws-secret-access-key": true, "x.pin": true, "credentials": true, "pwd": true,
		"keyboard": false, "passport": false, "token_count_x": true, "tokenizer": false, "dbname": false, "": false,
		"user": false, "host": false, "PIN": true, "pinned": false,
		"apikey": true, "secretkey": true, "accesskey": true, "privatekey": true, "clientsecret": true, "authtoken": true,
		"passphrase": true, "passcode": true, "pw": true, "secrets": false, "tokens": true, "keys": false,
		"password2": true, "Password1": true, "OAuth2Token": true, "token_v2": true, "creds": true,
		"monkey": false, "turkey": false, "auth": false, "bearer": false, "x-api-key": true, "DBPassword": true,
		// Words apart that aren't split at _, - or ., as a form's labels
		// and a path's names are.
		"Client Secret": true, "API Key": true, "db password": true, "aws/secret": true, "vault:token": true,
		"Secret Name": false, "sort key": false, "token url": false,
		// A count of them holds none.
		"token_count": false, "token count": false, "tokenCount": false,
	} {
		if got, _ := credentialKey(key); got != want {
			t.Errorf("credentialKey(%q) = %v, want %v (words %q)", key, got, want, words.Split(key))
		}
	}
	// A plural word decides only if no singular one does: a path under
	// credentials names one of them.
	for key, want := range map[string]bool{
		"credentials/db_password": false, "creds/db_password": false, "creds:api_token": false,
		"tokens/github_token": false, "Credentials db_password": false, "credentials.db_password": false,
		"credentials_db_password": false, "tokens": true, "api_keys": true, "db-credentials": true,
		"pin/client_keys": false, "secret.api_keys": false,
	} {
		if credential, isPlural := credentialKey(key); !credential || isPlural != want {
			t.Errorf("credentialKey(%q) = %v, plural %v, want a credential, plural %v (words %q)", key, credential, isPlural, want, words.Split(key))
		}
	}
	keys := []string{
		// kubeconfig
		"apiVersion", "kind", "clusters", "cluster", "server", "certificate-authority-data", "insecure-skip-tls-verify",
		"contexts", "context", "namespace", "current-context", "users", "user", "client-certificate-data", "client-key-data",
		"token", "auth-provider", "config", "cmd-path", "cmd-args", "expiry-key", "token-key", "exec", "args", "env", "preferences",
		// docker-compose
		"version", "services", "image", "ports", "volumes", "environment", "depends_on", "networks", "secrets", "configs",
		"healthcheck", "restart", "build", "dockerfile", "command", "entrypoint", "labels", "env_file", "deploy", "replicas",
		// AWS
		"aws_access_key_id", "aws_secret_access_key", "aws_session_token", "region", "output", "role_arn", "source_profile",
		"mfa_serial", "KeyName", "KeyType", "KeySchema", "AttributeName", "kms_key_id", "SSEKMSKeyId", "Bucket", "Key", "Prefix",
		// Terraform
		"terraform", "required_providers", "backend", "key", "bucket", "dynamodb_table", "encrypt", "key_name", "key_pair",
		"public_key", "private_key", "instance_type", "ami", "tags", "count", "for_each", "lifecycle", "keepers", "pin_version",
		// GitHub Actions
		"on", "jobs", "runs-on", "steps", "uses", "with", "permissions", "inherit", "fetch-depth", "ref",
		"github-token", "registry-url", "node-version", "cache", "cache-dependency-path", "persist-credentials",
		// package.json
		"name", "private", "scripts", "dependencies", "devDependencies", "keywords", "author", "license", "repository",
		"publishConfig", "engines", "bin", "main", "files", "workspaces", "resolutions", "overrides",
		// words with credential words inside, and keys that describe one
		"keyboard", "monkey", "turkey", "spin", "spinner", "pinned", "pinyin", "pinpoint", "passport", "passenger", "bypass",
		"compass", "passthrough", "pwm", "upward", "keystone", "keynote", "hotkeys", "tokenizer", "detokenize", "secretary",
		"credentialsProvider", "keepalive", "keyspace", "sortKey", "partitionKey", "primaryKey", "foreignKey", "cacheKey",
		"keyPrefix", "idempotencyKey", "routingKey", "shardKey", "dedupKey", "keyFile", "keyPath", "keyAlias", "keyId",
		"tokenUrl", "tokenEndpoint", "tokenType", "passwordPolicy", "passwordMinLength", "pinCode", "userpass", "apppw",
		"ownerpw", "idtoken", "rootkey", "accountkeys", "spw", "dbpwd", "pinned_version", "key_vault_name", "secret_name",
	}
	var got []string
	for _, k := range keys {
		if c, _ := credentialKey(k); c {
			got = append(got, k)
		}
	}
	want := []string{
		"client-key-data", "token", "aws_secret_access_key", "aws_session_token", "private_key", "github-token",
		"persist-credentials", "pinCode", "userpass", "apppw", "idtoken", "accountkeys", "dbpwd",
	}
	if !slices.Equal(got, want) {
		t.Errorf("credential keys %q, want %q", got, want)
	}
}

// What a structured secret holds under a credential key is a credential
// field: the key's value, or an item of a list that is, but not what a
// mapping under it holds, nor a plural key's value alone. Of common
// configs held as secrets, only their credentials are.
func TestCredentialFieldsConfigs(t *testing.T) {
	for name, tc := range map[string]struct {
		text string
		want []string
	}{
		"kubeconfig": {
			`{"apiVersion":"v1","kind":"Config","clusters":[{"name":"prod","cluster":{"server":"https://10.0.0.1:6443","certificate-authority-data":"LS0tLS1CRUd"}}],"users":[{"name":"admin","user":{"token":"eyJhbGciOi.payload.sig","client-key-data":"LS0tLS1CRUdJTiBSU0E"}}],"contexts":[{"name":"prod","context":{"cluster":"prod","user":"admin","namespace":"default"}}],"current-context":"prod"}`,
			[]string{"LS0tLS1CRUdJTiBSU0E", "eyJhbGciOi.payload.sig"},
		},
		"compose": {
			`{"version":"3.8","services":{"db":{"image":"postgres:16","environment":{"POSTGRES_PASSWORD_FILE":"/run/secrets/db_password","POSTGRES_USER":"app"},"secrets":["db_password"],"ports":["5432:5432"]}},"secrets":{"db_password":{"file":"./db_password.txt"}}}`,
			nil,
		},
		"aws": {
			`{"aws_access_key_id":"AKIAIOSFODNN7EXAMPLE","aws_secret_access_key":"wJalrXUtnFEMI/K7MDENG","region":"us-east-1","output":"json"}`,
			[]string{"wJalrXUtnFEMI/K7MDENG"},
		},
		"terraform": {
			`{"backend":{"s3":{"bucket":"tf-state","key":"prod/terraform.tfstate","region":"us-east-1","encrypt":true}},"key_name":"main","keys":["id","name","user"],"db_password":"hunt3r2","port":5432,"cache":{"key_prefix":"app","ttl":3600}}`,
			[]string{"hunt3r2"},
		},
		"actions": {
			`{"token":{"type":"bearer","scope":"repo"},"secrets":"inherit","with":{"fetch-depth":0,"token":"ghp_abcdefgh"}}`,
			[]string{"ghp_abcdefgh"},
		},
		"app": {
			`{"jwt":{"secret":"s3cr3tjwt","algorithm":"HS256","expires":3600},"redis":{"url":"redis://cache:6379","keyPrefix":"sess"},"auth":{"tokens":{"ttl":"1h","issuer":"auth"}},"pin":1234,"dbPassword":"pw12"}`,
			[]string{"1234", "pw12", "s3cr3tjwt"},
		},
		"lists": {
			`{"user":"svc","password":"hunt3r2","nested":{"apiKey":"k3y1","list":["a"]},"keys":["aaaa"],"tokens":["tok1","true"],"pin":"12","creds":{"value":"abcd"},"pin2":1234,"hosts":[{"name":"h1","pin":"4821"}]}`,
			[]string{"1234", "4821", "abcd", "hunt3r2", "k3y1", "tok1"},
		},
		"sections": {
			`{"secrets":{"db":"hunt3r","github":"ghp_abcdefghij","nested":{"file":"./db.txt"}},"credentials":{"username":"admin","value":"hunt3r2"}}`,
			[]string{"admin", "ghp_abcdefghij", "hunt3r", "hunt3r2"},
		},
		"compose sections": {`{"secrets":{"db_password":{"file":"./db_password.txt"}}}`, nil},
		"actions inherit":  {`{"secrets":"inherit"}`, nil},
		"not json":         {`not json`, nil},
	} {
		got := slices.Sorted(maps.Keys(credentialFields([]string{tc.text})))
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
}
