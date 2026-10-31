package problems

import "github.com/parallelworks/foundation/problem"

// The kinds of problem Meridian reports. A kind's code is part of the
// contract: never rename a code or one of its params. Its message is in
// messages/<language>.json, and its page's text in docs/<language>.json.
//
// Params that show a workflow's or a run's data hold it as data, which needs
// no translating:
//   - a name or a key as it is, which the message quotes, with its
//     language's quotation marks;
//   - a list of names or keys with each in neutral double quotes, joined
//     with ", " (see QuoteAll);
//   - a value as compact JSON (see Show), and a list of values as each
//     one's JSON, joined with ", ";
//   - a pattern or a piece of an expression as it is.
//
// A value that came from an organization or user variable or a password
// input, or was made from one, is never shown: a param that would show it
// is Masked.
//
// suggested is true when suggestion holds a name that was probably meant.

// Loading a workflow file.
var (
	FileTooLarge         = define(Error, "file_too_large", "File too large", "size", "limit")
	ExtraDocument        = define(Warning, "extra_document", "More than one YAML document")
	ExtraDocumentInvalid = define(Warning, "extra_document_invalid", "More than one YAML document, and the rest invalid", "reason")
	YAMLSyntax           = define(Error, "yaml_syntax", "Invalid YAML", "reason")
	YAMLAlias            = define(Error, "yaml_alias", "YAML alias")
	YAMLCollectionKey    = define(Error, "yaml_collection_key", "Key that isn't a string")
	YAMLMergeKey         = define(Error, "yaml_merge_key", "YAML merge key")
	DuplicateKey         = define(Warning, "duplicate_key", "Key given twice", "key", "first_line")
	NumberNotJSON        = define(Error, "number_not_json", "Number JSON can't hold", "value")
	FormatTooNew         = define(Error, "format_too_new", "Format too new", "format", "latest")
	// WorkflowTooLarge is a workflow read from a stream, such as standard
	// input, whose size isn't known: what was read passed limit.
	WorkflowTooLarge = define(Error, "workflow_too_large", "Workflow too large", "limit")
)

// Checking a workflow against its format's schema. The checks every API
// shares are foundation's rules, whose codes, messages and pages are
// foundation's too.
var (
	TooShort       = shared(problem.TooShort)       // min
	TooLong        = shared(problem.TooLong)        // max
	TooFewItems    = shared(problem.TooFew)         // min
	TooManyItems   = shared(problem.TooMany)        // max
	BelowMinimum   = shared(problem.BelowMinimum)   // min, exclusive
	AboveMaximum   = shared(problem.AboveMaximum)   // max, exclusive
	DuplicateItems = shared(problem.DuplicateItems) //
	InvalidChoice  = shared(problem.InvalidChoice)  // allowed
)

// Checking a workflow against its format's schema, with checks of its own.
var (
	SchemaFailed                 = define(Error, "schema_failed", "Schema check failed", "reason")
	UnknownKey                   = define(Warning, "unknown_key", "Unknown key", "key", "suggested", "suggestion")
	UnknownKeyUnresolved         = define(Error, "unknown_key_unresolved", "Unknown key", "key", "suggested", "suggestion")
	InvalidName                  = define(Error, "invalid_name", "Invalid name", "key", "patterns")
	InputTypeUnknown             = define(Error, "input_type_unknown", "Unknown input type", "type", "suggested", "suggestion", "types")
	WorkflowEmpty                = define(Error, "workflow_empty", "Empty workflow")
	WrongType                    = define(Error, "wrong_type", "Wrong type", "count", "type1", "type2", "type3", "has_got", "got")
	MissingKeys                  = define(Error, "missing_keys", "Missing keys", "keys")
	MissingAnyKey                = define(Error, "missing_any_key", "Missing one of some keys", "keys")
	NotExpression                = define(Error, "not_expression", "Not an expression", "value")
	PatternMismatch              = define(Error, "pattern_mismatch", "Doesn't match its pattern", "value", "pattern")
	MustEqual                    = define(Error, "must_equal", "Not the value allowed", "allowed")
	ValueMustEqual               = define(Error, "value_must_equal", "Not the value allowed", "value", "allowed")
	ValueMustBeOneOf             = define(Error, "value_must_be_one_of", "Not one of the values allowed", "value", "allowed")
	ValueMustEqualOrExpression   = define(Error, "value_must_equal_or_expression", "Not the value allowed or an expression", "value", "allowed")
	ValueMustBeOneOfOrExpression = define(Error, "value_must_be_one_of_or_expression", "Not one of the values allowed or an expression", "value", "allowed")
	ValueMustMatch               = define(Error, "value_must_match", "Doesn't match the patterns allowed", "value", "patterns")
	TooFewKeys                   = define(Error, "too_few_keys", "Too few keys", "count", "min")
	TooManyKeys                  = define(Error, "too_many_keys", "Too many keys", "count", "max")
	KeyNotAllowed                = define(Error, "key_not_allowed", "Key not allowed here", "key")
	KeysNotAllowedTogether       = define(Error, "keys_not_allowed_together", "Keys not allowed together", "keys")
	FormNotAllowed               = define(Error, "form_not_allowed", "Form not allowed here")
	SeveralForms                 = define(Error, "several_forms", "Matches several forms")
	NoForm                       = define(Error, "no_form", "Matches no form")
	SchemaRule                   = define(Error, "schema_rule", "Breaks a schema rule", "reason")
)

// A run's input values.
var (
	InputNotJSON           = define(Error, "input_not_json", "Input value that isn't JSON", "input", "what", "value")
	UnknownInput           = define(Warning, "unknown_input", "Value for an unknown input", "input", "suggested", "suggestion")
	InputGroupNotMapping   = define(Error, "input_group_not_mapping", "Group's value isn't a mapping", "input", "value")
	DefaultsReadEachOther  = define(Error, "defaults_read_each_other", "Defaults read each other", "inputs")
	InputRequired          = define(Error, "input_required", "Input needs a value", "input")
	InputConditionNotKnown = define(Error, "input_condition_not_known", "Input's condition not known", "key", "input", "name")
	PlatformInput          = define(Error, "platform_input", "Input needs a platform", "input", "type")
	PlatformValueNotJSON   = define(Error, "platform_value_not_json", "Platform's value isn't JSON", "input", "what", "value")
	InputRejected          = define(Error, "input_rejected", "Platform rejected an input's value", "input", "reason")
	InputNotNumber         = define(Error, "input_not_number", "Input takes a number", "input", "value")
	InputBelowMin          = define(Error, "input_below_min", "Input's value below its minimum", "input", "value", "min")
	InputAboveMax          = define(Error, "input_above_max", "Input's value above its maximum", "input", "value", "max")
	InputNotBoolean        = define(Error, "input_not_boolean", "Input takes true or false", "input", "value")
	InputNotList           = define(Error, "input_not_list", "Input takes a list", "input", "value")
	InputNotOptionList     = define(Error, "input_not_option_list", "Input takes a list of its options", "input", "value")
	InputNotOption         = define(Error, "input_not_option", "Not one of an input's options", "input", "value", "options")
	InputNotText           = define(Error, "input_not_text", "Input takes text", "input", "value")
	// InputReadAsSubmitted is a value submitted as another JSON type than
	// its input's, which format 1 reads as it was submitted: given is
	// text, number or boolean.
	InputReadAsSubmitted = define(Warning, "input_read_as_submitted", "Input read as it was submitted", "input", "type", "given")
	// FlattenedGroupValue is a value at the key of a group with flatten
	// true, whose inputs' values are beside it: input is the value's path,
	// and source run for a value the run was given, or row for one a list's
	// default row writes.
	FlattenedGroupValue = define(Warning, "flattened_group_value", "Value for a group that flattens", "input", "source")
	// FlattenedNameTaken is two input definitions whose values have one key,
	// where a group with flatten true puts one beside the other: input is
	// the key, and first and second the definitions' names, as in
	// group.name.
	FlattenedNameTaken = define(Error, "flattened_name_taken", "Two inputs with one value", "input", "first", "second")
	InputLooksSecret   = define(Warning, "input_looks_secret", "Input that looks like it holds a secret", "input")
	InputTooDeep       = define(Error, "input_too_deep", "Input's value nests too deeply", "input", "max")
	// DefaultRowUnknownInput is a key of a list's default row, written in
	// the workflow, that isn't one of its rows' inputs: input names the
	// list, and key the key's path in the row, as in group.name.
	DefaultRowUnknownInput = define(Warning, "default_row_unknown_input", "Default row with a key its rows don't have", "input", "key", "suggested", "suggestion")
	// ExecutionGroupUnknown is what the meridian command's Inputs gives
	// for an execution-groups input that names none of the user's groups:
	// value is the input's value, as Show writes it, and groups the groups
	// there are, as QuoteAll writes them.
	ExecutionGroupUnknown = define(Error, "execution_group_unknown", "No such execution group", "input", "value", "groups")
	// ClusterUnknown is what the meridian command's Inputs gives for a
	// compute-clusters input whose value names neither the workspace nor
	// one of the user's clusters, and ClustersNone when the user has none:
	// value is the name, as Show writes it, and clusters the clusters there
	// are, as QuoteAll writes them.
	ClusterUnknown = define(Error, "cluster_unknown", "No such cluster", "input", "value", "clusters")
	ClustersNone   = define(Error, "clusters_none", "No clusters to choose from", "input", "value")
	// BucketURIInvalid is what the meridian command's Inputs gives for a
	// bucket input whose value isn't a bucket's pw://user/name URI, as a
	// name alone or pw://name, which a platform reads as one of the user's
	// own buckets, and a run here has no platform user to read it as: value
	// is the value, as Show writes it. BucketCredentialsNeedPlatform is what
	// it gives for a bucket input with generateCredentials, whose access
	// keys only the platform that holds the bucket can make.
	BucketURIInvalid              = define(Error, "bucket_uri_invalid", "Not a bucket's pw:// URI", "input", "value")
	BucketCredentialsNeedPlatform = define(Error, "bucket_credentials_need_platform", "Bucket's credentials need a platform", "input")
)

// The workflows a step's uses: names, as a Sources resolver reports them.
// ref is the uses: as it is written; repo a repository, as owner/name;
// location a workflow's place, as a plan's Source names it; commit, digest
// and pinned as Git and the plan write them; and reason what git said.
var (
	SourceRefInvalid = define(Error, "source_ref_invalid", "Not a reference to a workflow in Git", "ref")
	// SourceWithInvalid is a step's with: $yaml or $host, key, that isn't a
	// file's path or a server's name: what is server or file.
	SourceWithInvalid = define(Error, "source_with_invalid", "Workflow's server or file not one a uses: can name", "key", "what")
	SourceOnPlatform  = define(Error, "source_on_platform", "Workflow a platform stores", "ref")
	// SourceVersionInvalid is a marketplace workflow's version, as a uses:
	// names it, that its slug's mapping in sources.yaml can't take; and
	// SourceLatestNotMapped one that names the latest, which only a mapping
	// of its own maps, whose key is key.
	SourceVersionInvalid   = define(Error, "source_version_invalid", "Marketplace workflow's version a mapping can't take", "ref", "version")
	SourceLatestNotMapped  = define(Error, "source_latest_not_mapped", "Marketplace workflow's latest version not mapped", "ref", "key")
	SourceRevisionNotFound = define(Error, "source_revision_not_found", "No such repository or revision", "repo", "revision", "reason")
	SourceFileNotFound     = define(Error, "source_file_not_found", "No such workflow file in the repository", "repo", "path", "commit")
	SourceTooLarge         = define(Error, "source_too_large", "Workflow file in a repository too large", "location", "commit", "limit")
	SourceChanged          = define(Error, "source_changed", "Workflow isn't the text it was pinned to", "location", "commit", "digest", "pinned")
	SourceNotPinned        = define(Error, "source_not_pinned", "Workflow not pinned", "location", "revision")
	// SourceServerUnknown is a GitLab server, as a step's with: $host
	// names it, that the user didn't register.
	SourceServerUnknown  = define(Error, "source_server_unknown", "GitLab server not registered", "server")
	SourceAccessDenied   = define(Error, "source_access_denied", "Access to a repository denied", "location", "reason")
	SourceObjectsInvalid = define(Error, "source_objects_invalid", "Malformed Git objects", "location", "reason")
	// The tree of workflows a run calls, as Create pins it: max is a
	// limit, chain the locations of the workflows on a path of calls, as
	// QuoteAll writes them, from the run's own, what what was counted,
	// calls or workflows, and input the last workflow's input, named as
	// input_required names it.
	WorkflowCallsItself   = define(Error, "workflow_calls_itself", "Workflow that calls itself", "chain")
	WorkflowNestedTooDeep = define(Error, "workflow_nested_too_deep", "Workflows called too deep", "max", "chain")
	WorkflowsTooMany      = define(Error, "workflows_too_many", "Too many workflows called", "max", "what")
	WorkflowsTooLarge     = define(Error, "workflows_too_large", "Workflows called too large", "max")
	ChildInputMissing     = define(Error, "child_input_missing", "Called workflow's required input not given", "input", "chain")
	// A child run of a step that calls a workflow: uses is the step's uses:,
	// and dir the child's directory.
	ChildNotPinned  = define(Error, "child_not_pinned", "Called workflow not pinned with its run's tree", "uses")
	ChildDirOutside = define(Error, "child_dir_outside", "Called workflow's directory outside its root run's", "dir")
	ChildNotCreated = define(Error, "child_not_created", "Called workflow's run not created")
)

// A workflow's jobs.
var (
	DuplicateStepID            = define(Error, "duplicate_step_id", "Two steps with the same id", "first", "second", "job", "id")
	NeedsNotKnown              = define(Error, "needs_not_known", "needs not known when the run is created", "name")
	NeedsItemNotName           = define(Error, "needs_item_not_name", "needs lists something other than a name", "value")
	NeedsNotName               = define(Error, "needs_not_name", "needs isn't a name or a list", "value")
	NeedsItself                = define(Error, "needs_itself", "Job needs itself", "job")
	NeedsUnknownJob            = define(Error, "needs_unknown_job", "Job needs a job that isn't there", "job", "needed", "suggested", "suggestion")
	NeedsCycle                 = define(Error, "needs_cycle", "Jobs need each other", "jobs")
	ContextNotKnown            = define(Error, "context_not_known", "Context not known here", "context", "phase")
	StatusNotKnown             = define(Error, "status_not_known", "Status not known here", "phase")
	RelativeOutsideDefinitions = define(Error, "relative_outside_definitions", "Relative reference outside input definitions")
	UndefinedInput             = define(Warning, "undefined_input", "No such input", "name", "suggested", "suggestion")
	UndefinedNestedInput       = define(Warning, "undefined_nested_input", "No such input in a group", "group", "name", "suggested", "suggestion")
	FlattenedInputRef          = define(Warning, "flattened_input_ref", "Input read through a container that flattens", "group", "name", "input")
	NeedsOutsideJob            = define(Error, "needs_outside_job", "needs outside a job")
	UndefinedJob               = define(Error, "undefined_job", "No such job", "job", "suggested", "suggestion")
	JobNotNeeded               = define(Error, "job_not_needed", "Job not needed", "job", "needed")
	NeededJobNoStep            = define(Error, "needed_job_no_step", "No such step in a needed job", "job", "id")
	NeedsUnknownKey            = define(Warning, "needs_unknown_key", "Unknown key of needs", "job", "key")
	NeedsReadWhole             = define(Warning, "needs_read_whole", "Needed job read whole", "job", "text")
	StepNotEarlier             = define(Error, "step_not_earlier", "Step doesn't run before this", "id")
	UndefinedStep              = define(Error, "undefined_step", "No such earlier step", "job", "id", "suggested", "suggestion")
	OutputUndefinedStep        = define(Warning, "output_undefined_step", "Output reads no such step", "job", "id", "suggested", "suggestion")
	StepsUnknownKey            = define(Warning, "steps_unknown_key", "Unknown key of a step", "id", "key")
	OutputStepsUnknownKey      = define(Warning, "output_steps_unknown_key", "Output reads an unknown key of a step", "id", "key")
	CleanupUndefinedStep       = define(Warning, "cleanup_undefined_step", "Cleanup reads no such step", "job", "id", "suggested", "suggestion")
	CleanupStepsUnknownKey     = define(Warning, "cleanup_steps_unknown_key", "Cleanup reads an unknown key of a step", "id", "key")
	StepsReadWhole             = define(Warning, "steps_read_whole", "Step read whole", "id", "text")
	NoMatrix                   = define(Warning, "no_matrix", "Job has no matrix", "job", "key", "text")
	UndefinedMatrixKey         = define(Warning, "undefined_matrix_key", "No such matrix key", "job", "key", "suggested", "suggestion")
	UndefinedSession           = define(Warning, "undefined_session", "No such session", "name", "suggested", "suggestion")
	UndefinedEnv               = define(Warning, "undefined_env", "No such env variable", "name", "suggested", "suggestion")
	VariableLooksSecret        = define(Warning, "variable_looks_secret", "Variable that looks like it holds a secret", "context", "name")
	ExpressionFailed           = define(Error, "expression_failed", "Expression failed", "reason")
	WordNotReference           = define(Warning, "word_not_reference", "Word that looks like a reference", "word", "text", "head")
	HyphenatedName             = define(Warning, "hyphenated_name", "Hyphenated name without brackets", "written", "word", "text", "name", "fix", "defines", "defined", "job")
	ConditionWord              = define(Warning, "condition_word", "Word that decides a condition", "word", "text")
	OlderAlways                = define(Warning, "older_always", "Older spelling of always()")
	ReplacedKeyword            = define(Warning, "replaced_keyword", "Replaced if: keyword", "word", "text", "replacement")
	ValueTooDeep               = define(Error, "value_too_deep", "Value nests too deeply", "max")
	// SSHDisconnectTimeoutIgnored is a format 1 ssh:'s disconnect-timeout
	// that isn't negative, which Meridian doesn't honour: a step keeps
	// running through a lost connection to its host, as format 1 runs one
	// with a negative disconnect-timeout. value is the key's value, as the
	// workflow writes it.
	SSHDisconnectTimeoutIgnored = define(Warning, "ssh_disconnect_timeout_ignored", "disconnect-timeout not honoured", "value")
	// SSHDisconnectTimeoutInvalid is a format 1 ssh:'s disconnect-timeout
	// that format 1 can't read, as 1h30m, for which it fails the step:
	// Meridian ignores the key, and runs it. value is the key's value, as
	// the workflow writes it.
	SSHDisconnectTimeoutInvalid = define(Warning, "ssh_disconnect_timeout_invalid", "disconnect-timeout format 1 can't read", "value")
)

// A workflow's sessions:.
var (
	// SessionRedirectTwice is more than one session whose redirect: is
	// true, of which format 1 allows one: keys are their keys, as QuoteAll
	// writes them.
	SessionRedirectTwice = define(Error, "session_redirect_twice", "More than one session to take the user to", "keys")
)

// Serving a run's sessions on this machine, which a session provider
// refuses the run, or the step that points a session, with: user is the
// login name of the user who runs Meridian, as it is; port a port of
// 127.0.0.1, as text.
var (
	SessionUserUnsupported = define(Error, "session_user_unsupported", "Login name a session's path can't hold", "user")
	// name is a session's name, as a run's sessions: named it, or as
	// update-session's with: gives it.
	SessionNameTaken    = define(Error, "session_name_taken", "Session name another session has", "name")
	SessionNameEmpty    = define(Error, "session_name_empty", "update-session without a session's name")
	SessionUnknown      = define(Error, "session_unknown", "No such session", "name")
	SessionNotPermitted = define(Error, "session_not_permitted", "Session the run may not point", "name")
	// key is a session's key in a workflow's sessions:, and field one of
	// its keys, or of update-session's with:; target is update-session's
	// target, clusters the clusters of the clusters file, as QuoteAll
	// writes them, and listed whether there are any, "true" or "false";
	// value is a value of update-session's with:, as Show writes it, and
	// name a session's name.
	SessionFieldUnsupported  = define(Error, "session_field_unsupported", "Session meridian run can't serve", "key", "field")
	SessionNameNeeded        = define(Error, "session_name_needed", "Session that asks for a name", "key")
	SessionUpdateUnsupported = define(Error, "session_update_unsupported", "update-session meridian run can't do", "name", "field")
	SessionURLInvalid        = define(Error, "session_url_invalid", "Link that isn't an http or https URL", "value")
	SessionPortInvalid       = define(Error, "session_port_invalid", "Port update-session can't read", "key", "value")
	SessionTargetUnknown     = define(Error, "session_target_unknown", "Session's target that names no cluster", "target", "clusters", "listed")
	SessionTLSUnsupported    = define(Error, "session_tls_unsupported", "Session over TLS meridian run can't reach yet", "name")
	// port is a session's port, and picked whether the system picked it,
	// "true" or "false", as for port 0.
	SessionLocalPortTaken   = define(Error, "session_local_port_taken", "Session's port in use", "port", "picked")
	SessionLocalPortRefused = define(Error, "session_local_port_refused", "Session's port this machine won't listen on", "port", "picked")
	// reason is why the system gives no login name: a session can't be
	// named, nor a step point one, nor meridian session url link one.
	LoginNameUnknown = define(Error, "login_name_unknown", "No login name for the user who runs meridian", "reason")
)

// Creating and starting a run: what the caller gives the compiler, and
// what Start decides again.
var (
	// VariableNotText is a variable the run reads that the compiler was
	// given a value for that isn't text: context is org or vars, and name
	// the variable's name.
	VariableNotText = define(Warning, "variable_not_text", "Variable that isn't text", "context", "name")
	// ResolverMissing is a key of the workflow that needs one of the
	// interfaces whoever runs the workflow provides, which isn't there:
	// key is the key, as sessions, and interface the interface's name, as
	// Sessions (docs/design.md, Interfaces).
	ResolverMissing = define(Error, "resolver_missing", "Interface missing", "key", "interface")
	// RunShapeChanged is a decision of the run's shape that Start made
	// otherwise than Create did: key is hidden, ignore or optional, and
	// name the input's name, or key is needs, and name the job's.
	RunShapeChanged = define(Error, "run_shape_changed", "Run's shape changed as it started", "key", "name")
	// ValuesReadEachOther is values that Start evaluates and that read
	// each other: names are their names, as inputs.region and env.REGION,
	// as QuoteAll writes them.
	ValuesReadEachOther = define(Error, "values_read_each_other", "Values read each other as the run starts", "names")
	// GroupUnknown is an execution group a job's runs-on: names that the
	// Groups resolver doesn't have: group is its name.
	GroupUnknown = define(Error, "group_unknown", "No such execution group", "group")
	// EnvironmentUnknown is an environment a format 1 runs-on: names that
	// the Groups resolver doesn't have: cluster and name are the
	// environment's.
	EnvironmentUnknown = define(Error, "environment_unknown", "No such environment", "cluster", "name")
	// ComputeTargetUnknown is a compute target a format 1 runs-on: writes
	// out that the Groups resolver doesn't have: mode and target are its
	// mode and targetId.
	ComputeTargetUnknown = define(Error, "compute_target_unknown", "No such compute target", "mode", "target")
	// SourceNotFound is a workflow a step's uses: names that the Sources
	// resolver doesn't have, without saying why: ref is the uses:.
	SourceNotFound = define(Error, "source_not_found", "No such workflow", "ref")
	// SourceNotKnown is a step's with: $yaml or $host, key, that isn't
	// written out, though the workflow its uses: names is pinned as the
	// run is created.
	SourceNotKnown = define(Error, "source_not_known", "Workflow's place not known when the run is created", "key")
	// CheckoutNotKnown is a parallelworks/checkout step's with: repo or
	// branch, key, that isn't written out, though the step is pinned to a
	// commit as the run is created.
	CheckoutNotKnown = define(Error, "checkout_not_known", "Checkout's repository or revision not known when the run is created", "key")
	// CheckoutNotFound is a revision of a repository a
	// parallelworks/checkout step names that the Sources resolver doesn't
	// have, without saying why.
	CheckoutNotFound = define(Error, "checkout_not_found", "No such repository or revision to check out", "repo", "revision")
	// CheckoutRepoInvalid is a parallelworks/checkout step's with: repo that
	// Meridian doesn't fetch: repo is it as a problem shows it, with no
	// credential, and reason why, transport, credential, form or name
	// (plan.CheckRepo).
	CheckoutRepoInvalid = define(Error, "checkout_repo_invalid", "Checkout of a repository Meridian doesn't fetch", "repo", "reason")
	// CheckoutPathInvalid is an entry of a parallelworks/checkout step's
	// with: sparse_checkout, path, that isn't a literal path from the
	// repository's root (plan.CheckCheckoutPath).
	CheckoutPathInvalid = define(Error, "checkout_path_invalid", "Sparse checkout entry that isn't a path", "path")
	// CheckoutRevisionInvalid is a parallelworks/checkout step's with:
	// branch, revision, that a Sources resolver doesn't ask git for: one git
	// could take for an option, or refuses as a branch's or a tag's name.
	CheckoutRevisionInvalid = define(Error, "checkout_revision_invalid", "Checkout's revision that isn't a name git takes", "revision")
)

// Running a workflow: what the phases after Start evaluate, and the
// matrices they expand.
var (
	RunsOnInvalid         = define(Error, "runs_on_invalid", "runs-on isn't a place to run", "value")
	ResourcesInvalid      = define(Error, "resources_invalid", "Resource that isn't a quantity", "key", "value")
	DurationInvalid       = define(Error, "duration_invalid", "Not a duration", "key", "value")
	CountInvalid          = define(Error, "count_invalid", "Not a number of times", "key", "value")
	MatrixNotList         = define(Error, "matrix_not_list", "Matrix values that aren't a list", "key", "value")
	MatrixEntryNotMapping = define(Error, "matrix_entry_not_mapping", "Matrix entry that isn't a mapping", "key", "value")
	MatrixEmpty           = define(Error, "matrix_empty", "Matrix without combinations")
	MatrixTooLarge        = define(Error, "matrix_too_large", "Matrix with too many combinations", "limit")
	OutputNotWritten      = define(Error, "output_not_written", "Output that nothing wrote", "of", "owner", "name")
	WorkingDirectoryVar   = define(Error, "working_directory_variable", "Shell variable in a working directory", "variable")
	// JobSSHHostEmpty is a job whose format 1 ssh: gives a remoteHost that
	// is empty or null, which runs where it would without its ssh:, as
	// format 1 runs it. Ready reports it, and the job's problems keep it.
	JobSSHHostEmpty = define(Warning, "job_ssh_host_empty", "Job's ssh: names no host")
)

// Parsing an expression.
var (
	ExpressionEmpty     = define(Error, "expression_empty", "Empty expression")
	ExpressionTooDeep   = define(Error, "expression_too_deep", "Expression nests too deeply", "max")
	ExpressionEndsEarly = define(Error, "expression_ends_early", "Expression ends too soon")
	UnexpectedText      = define(Error, "unexpected_text", "Unexpected text", "text")
	TernaryMissingColon = define(Error, "ternary_missing_colon", "? without its :", "at")
	CoalesceMixed       = define(Error, "coalesce_mixed", "?? mixed with || or &&")
	InvalidJSONLiteral  = define(Error, "invalid_json_literal", "Invalid JSON in an expression", "text")
	NotAFunction        = define(Error, "not_a_function", "Not a function", "name")
	CallMissingComma    = define(Error, "call_missing_comma", "Call missing , or )", "name")
	WrongArgumentCount  = define(Error, "wrong_argument_count", "Wrong number of arguments", "name", "arity", "min", "max", "count")
	CaseArguments       = define(Error, "case_arguments", "case() arguments")
	ExpectedCloseParen  = define(Error, "expected_close_paren", "Missing )")
	UnclosedString      = define(Error, "unclosed_string", "Unclosed string", "quote")
	UnclosedBracket     = define(Error, "unclosed_bracket", "Unclosed bracket", "bracket")
	UnclosedTemplate    = define(Error, "unclosed_template", "${{ without }}")

	// map() and filter()'s lambdas.
	LambdaExpected          = define(Error, "lambda_expected", "Lambda expected", "name")
	LambdaTooManyParameters = define(Error, "lambda_too_many_parameters", "Lambda with too many parameters", "name", "count")
	LambdaParameterReserved = define(Error, "lambda_parameter_reserved", "Lambda parameter with a reserved name", "name")
	LambdaParameterTwice    = define(Error, "lambda_parameter_twice", "Lambda parameters with the same name", "name")
	LambdaMisplaced         = define(Error, "lambda_misplaced", "Lambda outside map() or filter()")
)

// Evaluating an expression.
var (
	FormatUnclosedBrace      = define(Error, "format_unclosed_brace", "format() template with an unclosed {", "template")
	FormatMissingArgument    = define(Error, "format_missing_argument", "format() template without an argument", "template", "placeholder")
	FormatUnopenedBrace      = define(Error, "format_unopened_brace", "format() template with an unopened }", "template")
	RangeNotWhole            = define(Error, "range_not_whole", "range of numbers that aren't whole", "start", "end")
	RangeOutOfBounds         = define(Error, "range_out_of_bounds", "range of numbers too large", "start", "end")
	RangeTooLong             = define(Error, "range_too_long", "range too long", "start", "end", "max")
	FromJSONInvalid          = define(Error, "from_json_invalid", "fromJSON of invalid JSON", "text")
	UnknownOperator          = define(Error, "unknown_operator", "Unknown operator", "op")
	ExpressionTooLarge       = define(Error, "expression_too_large", "Expression builds too much", "limit")
	ExpressionTooMuchWork    = define(Error, "expression_too_much_work", "Expression does too much work", "limit")
	ExpressionsReadEachOther = define(Error, "expressions_read_each_other", "Expressions read each other")

	// map() and filter().
	NotListOrMapping = define(Error, "not_list_or_mapping", "map() or filter() of what isn't a list or mapping", "name", "value")
)

// Meridian's own limits on evaluating expressions, which aren't part of the
// format: expressions that are each within the language's limits can still
// pass them together.
var (
	ExpressionsBuildTooMuch = define(Error, "expressions_build_too_much", "Expressions build too much together", "limit")
	ExpressionsTooMuchWork  = define(Error, "expressions_too_much_work", "Expressions do too much work together", "limit")
	ExpressionsTooSlow      = define(Error, "expressions_too_slow", "Expressions take too long together", "limit")
)

// Meridian's own limit on the problems of each kind it lists, which counts
// the rest: a workflow, or a run's submitted values, can have more than any
// reader goes through, and a plan keeps its warnings.
var (
	ProblemsNotListed = define(Warning, "problems_not_listed", "More problems of one kind", "code", "count", "limit")
)

// Running a job on a worker. A step's are diagnostics of its finished
// event, at the step's path; the setup's are the problem with which an
// attempt that failed before its first step ends, and is placed again.
var (
	ContainerNotSupported = define(Error, "container_not_supported", "Container the worker can't run yet", "runtime", "image")
	JobUnreadable         = define(Error, "job_unreadable", "Job the worker couldn't read")
	JobVersionUnsupported = define(Error, "job_version_unsupported", "Job of a plan version the worker doesn't read", "version", "supported")
	RunDirUnusable        = define(Error, "run_dir_unusable", "Run directory the worker couldn't use", "path", "reason")
	EnvNameInvalid        = define(Error, "env_name_invalid", "Platform's variable with a name no variable can have", "name")
	UsesNotSupported      = define(Error, "uses_not_supported", "uses: step an earlier version of Meridian can't run", "uses")
	ActionNeedsPlatform   = define(Error, "action_needs_platform", "Built-in action only a platform provides", "uses")
	ActionRequestFailed   = define(Error, "action_request_failed", "Request to the coordinator for a built-in action that failed", "uses", "reason")
	CancelJobsJobUnknown  = define(Warning, "cancel_jobs_job_unknown", "Job a step can't cancel", "job", "suggested", "suggestion")
	CancelJobsOtherRun    = define(Error, "cancel_jobs_other_run", "Cancel of another run's jobs", "key")
	CancelJobsRunMissing  = define(Error, "cancel_jobs_run_missing", "Cancel of a workflow's run without its number")
	ChildRequestFailed    = define(Error, "child_request_failed", "Request to the coordinator for a called workflow's run that failed", "uses", "reason")
	ChildDirUnusable      = define(Error, "child_dir_unusable", "Called workflow's directory the worker couldn't make", "dir", "reason")
	// StepRunRequestFailed is a request to the coordinator, of a step's
	// try, or its cleanup's, that runs on another host, for its run there,
	// that failed for good, for reason; StepRunOutputsUnwritten what such a
	// step wrote to $OUTPUTS there, which the worker couldn't add to the
	// file the step would have written, for reason; and
	// StepRunOutputLineTooLong the output name of it, whose line in that
	// file, escaped, would be longer than limit bytes.
	StepRunRequestFailed     = define(Error, "step_run_request_failed", "Request to the coordinator for a step's run on another host that failed", "reason")
	StepRunOutputsUnwritten  = define(Error, "step_run_outputs_unwritten", "Outputs of a step on another host the worker couldn't add", "reason")
	StepRunOutputLineTooLong = define(Error, "step_run_output_line_too_long", "Output of a step on another host too long to add", "name", "limit")
	// MasksTooMany is a step's try, or its cleanup's, that calls a
	// workflow or runs on another host, whose run of its own would mask
	// count strings, more than limit, the most the coordinator takes
	// (workerapi.MaxMasks); and MasksTooLarge one whose strings to mask
	// come to size bytes, more than limit (workerapi.MaxMaskedBytes). The
	// worker fails the step rather than ask for the run with fewer.
	MasksTooMany  = define(Error, "masks_too_many", "Too many strings to mask for a step's run of its own", "count", "limit")
	MasksTooLarge = define(Error, "masks_too_large", "Strings to mask too large for a step's run of its own", "size", "limit")
	// CheckoutPathNotFound is an entry of a parallelworks/checkout step's
	// with: sparse_checkout, path, that names nothing in the commit the
	// step was pinned to; CheckoutSubmodules a checkout of all of the
	// repository repo, as the step writes it, which has submodules;
	// CheckoutTooLarge one whose files come to size bytes, more than
	// limit, and CheckoutTooMany one with count entries, more than limit;
	// and CheckoutUnavailable one whose commit, of repo, the coordinator
	// couldn't read, for reason. The coordinator answers them as the step
	// asks for its action, and the worker for a tree that says otherwise.
	CheckoutPathNotFound = define(Warning, "checkout_path_not_found", "Sparse checkout entry that names nothing", "path")
	CheckoutSubmodules   = define(Error, "checkout_submodules", "Whole checkout of a repository with submodules", "repo")
	CheckoutTooLarge     = define(Error, "checkout_too_large", "Checkout too large", "size", "limit")
	CheckoutTooMany      = define(Error, "checkout_too_many", "Checkout with too many entries", "count", "limit")
	CheckoutUnavailable  = define(Error, "checkout_unavailable", "Checkout's commit that couldn't be read", "repo", "commit", "reason")
	// CheckoutUnsafe is a member of a checkout's tree, path, that the
	// worker doesn't write, for reason: name, for one whose name leaves
	// the step's directory, or is .git, link, for a symbolic link to .git,
	// and type, for one that is neither a file, a directory nor a symbolic
	// link; and CheckoutWriteFailed one
	// the system refused to write, for reason.
	CheckoutUnsafe      = define(Error, "checkout_unsafe", "Checkout's tree member the worker doesn't write", "path", "reason")
	CheckoutWriteFailed = define(Error, "checkout_write_failed", "Checkout's tree member the worker couldn't write", "path", "reason")
	StepSSHNotSupported = define(Error, "step_ssh_not_supported", "Step on another host")
	StepSSHReadsWorker  = define(Error, "step_ssh_reads_worker", "Step's ssh: that reads what only its job's worker knows", "ref")
	StepOffJobHost      = define(Error, "step_off_job_host", "Step to run off its job's host")
	SSHHostEmpty        = define(Warning, "ssh_host_empty", "Step's ssh: names no host")
	StepNotStarted      = define(Error, "step_not_started", "Step's command not started", "reason")
	EvaluationFailed    = define(Error, "evaluation_failed", "Step or job the worker couldn't evaluate")
	WorkerDirUnusable   = define(Error, "worker_dir_unusable", "No directory of the worker's own for a step's files", "path", "code")
	ShellNotSupported   = define(Error, "shell_not_supported", "Shell format 1 doesn't run", "shell")
	StepTimedOut        = define(Error, "step_timed_out", "Step ran past its timeout", "limit")
	OutputsTooLarge     = define(Error, "outputs_too_large", "Step's outputs too large", "size", "limit")
	OutputsLineTooLong  = define(Warning, "outputs_line_too_long", "Line of $OUTPUTS too long", "limit")
	OutputsNotUTF8      = define(Warning, "outputs_not_utf8", "Outputs that aren't UTF-8", "name", "count")
	OutputsNotFile      = define(Error, "outputs_not_file", "$OUTPUTS isn't a file")
	OutputsRemoved      = define(Warning, "outputs_removed", "$OUTPUTS removed as the step ran")
	LogNotKept          = define(Warning, "log_not_kept", "Log not kept whole", "size", "limit")
	ReportNotEncoded    = define(Error, "report_not_encoded", "Report the worker couldn't encode")
)

// The coordinator's problems with a run, which fail it: run is the run's
// ID.
var (
	// RecordUnreadable is a run's secret record that doesn't decrypt as
	// the run's latest: one sealed for another run or another store, one
	// that a later one replaced, or one changed.
	RecordUnreadable = define(Error, "record_unreadable", "Secret record that can't be decrypted", "run")
	// RecordResealLimit is a run's secret record that has been encrypted
	// again limit times, as many as its data key is used for.
	RecordResealLimit = define(Error, "record_reseal_limit", "Secret record encrypted too many times", "run", "limit")
	// RunRecordMismatch is a run whose secret record doesn't fit its plan
	// (compile.ErrMismatch).
	RunRecordMismatch = define(Error, "run_record_mismatch", "Secret record that doesn't fit the run's plan", "run")
	// RunStartUnanswered is a run whose Start couldn't read what it reads,
	// a variable or a platform's answer, in tries tries.
	RunStartUnanswered = define(Error, "run_start_unanswered", "Run that couldn't start", "run", "tries")
	// RunMalformed is a run whose stored plan or secret record names no
	// version, or doesn't decode as the version it names.
	RunMalformed = define(Error, "run_malformed", "Run that can't be read", "run")
	// RunNotStarted is a run whose Start the coordinator gave up on.
	RunNotStarted = define(Error, "run_not_started", "Run that ended without starting", "run")
	// LogTruncated is a log the coordinator kept only the first bytes of,
	// as its attempt's logs reached limit bytes in all: the line that ends
	// it says so.
	LogTruncated = define(Warning, "log_truncated", "Log the coordinator didn't keep whole", "limit")
	// LogCut is a log, of the step or the setup log names, whose attempt
	// ended before the log did, as one whose worker was lost: what its
	// worker didn't store of it, it never will.
	LogCut = define(Warning, "log_cut", "Log cut short", "log")
)

// RunPlanNewer is a run whose plan is of version, newer than latest, the
// latest this release reads, or holds what only a newer release reads. It
// doesn't fail the run: the coordinator leaves it to a release that reads
// its plan, and says so.
var RunPlanNewer = define(Warning, "run_plan_newer", "Run newer than this release starts", "run", "version", "latest")

// The problems of placing a job, and of seeing it to its end.
var (
	// PlacementImpossible is a job that no member of its group can run, as
	// their drivers' Probe said: each member's reason follows it.
	PlacementImpossible = define(Error, "placement_impossible", "Job no member can run", "job")
	// PlacementUnanswered is a job whose group's backends couldn't answer
	// as it was placed, tries times in a row, which counts as a failure
	// before its first step.
	PlacementUnanswered = define(Error, "placement_unanswered", "Job that couldn't be placed", "job", "tries")
	// SetupFailed is a job whose attempts failed before a step started,
	// tries times, the last of which, or twice for the same reason, which
	// follows it.
	SetupFailed = define(Error, "setup_failed", "Job that failed to start", "job", "tries")
	// CoordinatorJobFailed is a job the coordinator couldn't see to its
	// end, as one whose supervisor failed again and again.
	CoordinatorJobFailed = define(Error, "coordinator_job_failed", "Job the coordinator couldn't run", "job")
	// JobTimeoutEnforced is a job past its timeout: whose worker didn't
	// stop it, which the coordinator stopped, as a backstop.
	JobTimeoutEnforced = define(Error, "job_timeout_enforced", "Job the coordinator stopped at its timeout", "job")
)

// The problems of a job whose worker was lost (docs/design.md, Leases and
// fencing): reason is why its scheduler, or the kernel, ended it, timeout
// or out-of-memory; and retries how many times the job's retry: had run it
// again.
var (
	// WorkerLost is a job whose worker was lost after a step started,
	// which its retry: doesn't run again.
	WorkerLost = define(Error, "worker_lost", "Job whose worker was lost", "job")
	// WorkerLostAgain is a job whose worker was lost again, once its
	// retry: had run it again as many times as it allows.
	WorkerLostAgain = define(Error, "worker_lost_again", "Job whose worker was lost more times than its retry allows", "job", "retries")
	// WorkerEndedEarly is an attempt whose worker ended before its job's
	// first step, a failure before the first step, which places the job
	// again.
	WorkerEndedEarly = define(Error, "worker_ended_early", "Worker that ended before the job's first step", "job")
	// WorkerKilled is a job whose worker its scheduler, or the kernel,
	// ended: at its time limit, or for the memory it used.
	WorkerKilled = define(Error, "worker_killed", "Worker its scheduler ended", "job", "reason")
	// MemberGone is an attempt placed on a member that its group no longer
	// has, whose backend can't say whether its worker runs: a failure
	// before the job's first step, which places the job again.
	MemberGone = define(Error, "member_gone", "Attempt whose member its group no longer has", "job")
)

// The meridian command's own problems, in CommandRegistry. A path, an
// environment variable's name or value, or what the operating system said,
// reason, is a param as it is.

// Placing a job's attempt on a member of its group, through the member's
// backend (package backend), whose Fit or error the attempt records:
// member is the member's name; isolation what it names, which
// has_isolation says it does; resource is cpu, memory or gpu, with asks
// and offers in thousandths of a core, bytes or GPUs; backend is the
// member's; path is the meridian binary's that would start the worker, and
// code the problem with it, whose page says why; ended says whether a
// worker that didn't take its token ended first, with the exit status
// exit, or didn't ask for it in time, and log is the file its standard
// error is in.
var (
	IsolationNotBuilt     = define(Error, "isolation_not_built", "Isolation its backend can't give yet", "member", "isolation", "has_isolation")
	ResourcesExceedMember = define(Error, "resources_exceed_member", "Job asks for more than its member offers", "member", "resource", "asks", "offers")
	BackendNotBuilt       = define(Error, "backend_not_built", "Backend this release doesn't have yet", "member", "backend")
	WorkerBinaryUnsafe    = define(Error, "worker_binary_unsafe", "Meridian binary that isn't safe to start", "path", "code")
	WorkerTokenNotTaken   = define(Error, "worker_token_not_taken", "Worker that didn't take its token", "ended", "exit", "log")
)

// The ssh backend's problems with a host (meridian host), whose Fit or
// error the attempt records: host is the host's name, as it says it; path
// a directory there, or a command; at the path where its check failed, the
// directory or one on the way to it; reason what is wrong with it, a
// problem's code, whose page says why, or the system's error; code a
// problem's code; and version a systemd's version, minimum the first that
// will do.
var (
	SSHWorkerWouldBeKilled = define(Error, "ssh_worker_would_be_killed", "Host that would kill a worker as its session ends", "host")
	SSHHostDirUnusable     = define(Error, "ssh_host_dir_unusable", "Directory on a host that Meridian can't use", "host", "path", "at", "reason")
	SSHHostCommandUnsafe   = define(Error, "ssh_host_command_unsafe", "Command on a host that isn't safe to run", "host", "path", "code")
	SSHHostIDUnknown       = define(Error, "ssh_host_id_unknown", "Host without an ID of its own", "host")
	SSHLoginctlMissing     = define(Error, "ssh_loginctl_missing", "Host whose logind can't be asked", "host")
	SSHSystemdTooOld       = define(Error, "ssh_systemd_too_old", "Host whose systemd can't start a worker's scope", "host", "version", "minimum")
)

// Connecting to a host over SSH (backend/ssh): host is the host as it was
// named, an ssh_config alias, a name or an address, or host:port; type and
// fingerprint are its key's, as ssh-keygen -l shows them; file and line
// are where known_hosts has the key it differs from; category is which of
// its algorithms the host offers none Meridian may use of, key-exchange,
// host-key, cipher or mac, and offered what it offers; user is who logs
// in, and keys how many keys were offered; reason is what ssh, or the
// system, said; key and value are a format 1 ssh: key and what it gives;
// file is a known_hosts file, or a RevokedHostKeys file; token is a
// ProxyCommand's, as %x, and hop one of a ProxyJump's hosts, as it writes
// it; limit is how deep ProxyJumps may nest; keyword is a keyword of
// ssh -G's output, as it prints it; path is an ssh_config, or a command
// Meridian runs to connect; and code is the code of the problem with it.
var (
	SSHTargetUnsafe              = define(Error, "ssh_target_unsafe", "Host or user that isn't safe to give ssh", "key", "value")
	SSHHostFormUnsupported       = define(Error, "ssh_host_form_unsupported", "Host in a form Meridian doesn't connect to", "key", "value")
	SSHKnownHostsUnmatched       = define(Error, "ssh_known_hosts_unmatched", "known_hosts with hashed lines that can't be matched", "host", "file", "reason")
	SSHHostCertificateInvalid    = define(Error, "ssh_host_certificate_invalid", "Host certificate that its authority's signature doesn't make valid", "host", "fingerprint", "reason")
	SSHProxyCommandToken         = define(Error, "ssh_proxy_command_token", "ProxyCommand with a token Meridian doesn't expand", "host", "token")
	SSHProxyJumpTooDeep          = define(Error, "ssh_proxy_jump_too_deep", "ProxyJumps that nest too deep", "host", "limit")
	SSHProxyJumpInvalid          = define(Error, "ssh_proxy_jump_invalid", "ProxyJump host that names no host, or isn't safe to give ssh", "host", "hop")
	SSHHostKeyUnknown            = define(Error, "ssh_host_key_unknown", "Host key that known_hosts doesn't have", "host", "type", "fingerprint")
	SSHHostKeyChanged            = define(Error, "ssh_host_key_changed", "Host key that isn't the one known_hosts has", "host", "type", "fingerprint", "file", "line")
	SSHHostKeyRevoked            = define(Error, "ssh_host_key_revoked", "Host key that is revoked", "host", "fingerprint")
	SSHAlgorithmsUnapproved      = define(Error, "ssh_algorithms_unapproved", "Host that offers no algorithm FIPS 140-3 approves", "host", "category", "offered")
	SSHAuthFailed                = define(Error, "ssh_auth_failed", "Login to a host refused", "host", "user", "keys")
	SSHConfigUnreadable          = define(Error, "ssh_config_unreadable", "SSH configuration that can't be read", "host", "reason")
	SSHConfigNotRead             = define(Warning, "ssh_config_not_read", "SSH configuration not read, with no ssh client", "host")
	SSHConfigOutputUnexpected    = define(Error, "ssh_config_output_unexpected", "ssh -G output that Meridian doesn't trust", "host", "keyword")
	SSHRevokedHostKeysUnreadable = define(Error, "ssh_revoked_host_keys_unreadable", "Revoked host keys that can't be read", "host", "file", "reason")
	SSHKnownHostsCommand         = define(Error, "ssh_known_hosts_command", "KnownHostsCommand, which Meridian doesn't run", "host")
	SSHClientMissing             = define(Error, "ssh_client_missing", "SSH configuration without an ssh client to read it", "host", "path")
	SSHCommandUnsafe             = define(Error, "ssh_command_unsafe", "Command to connect with that isn't safe to run", "path", "code")
)

// Putting the worker's binary on a host (backend/ssh): host is the host as
// it was named; os and arch its system, as uname, or Go, names them;
// version this release's; path where the binary is looked for, or was
// copied to, and digest its digest; and found the digest of the copy on
// the host, or none, when the host has no tool to compute one with.
var (
	SSHHostUnsupported        = define(Error, "ssh_host_unsupported", "Host of a system the worker doesn't run on", "host", "os", "arch")
	SSHWorkerBinaryMissing    = define(Error, "ssh_worker_binary_missing", "No worker binary for a host's system", "host", "os", "arch", "version", "path")
	SSHWorkerBinaryUnverified = define(Error, "ssh_worker_binary_unverified", "Worker binary on a host that isn't the one copied", "host", "path", "digest", "found")
)

// The ssh backend's driver (backend/ssh): member is a member's name, and
// host its host as it was named; reason why the host couldn't be reached,
// as the system or ssh said, or what meridian host said on its standard
// error; and op the operation of meridian host's that was asked for.
var (
	SSHHostMissing      = define(Error, "ssh_host_missing", "Member of the ssh backend that names no host", "member")
	SSHUnreachable      = define(Error, "ssh_unreachable", "Host that can't be reached", "host", "reason")
	SSHHostHelperFailed = define(Error, "ssh_host_helper_failed", "meridian host that stopped before it answered", "host", "op", "reason")
	// release is the release meridian host says it is, and expected the
	// coordinator's.
	SSHHostReleaseMismatch = define(Error, "ssh_host_release_mismatch", "meridian host of another release", "host", "release", "expected")
	SSHPlacementElsewhere  = define(Error, "ssh_placement_elsewhere", "Host whose connection reaches another node than a worker's", "host")
	// path is the socket on the host that the coordinator asked sshd to
	// listen on for the host's workers.
	SSHForwardingRefused = define(Error, "ssh_forwarding_refused", "Host that won't forward a socket to the coordinator", "host", "path")
	// address is what the host was asked to connect to, as host:port, and
	// reason what the host said of why it couldn't; value and port a host
	// and a port, as they were given, which Meridian asks no host to
	// connect to.
	SSHTCPForwardingRefused = define(Error, "ssh_tcp_forwarding_refused", "Host that won't connect to an address for Meridian", "host", "address")
	SSHTCPConnectFailed     = define(Error, "ssh_tcp_connect_failed", "Host that couldn't connect to an address for Meridian", "host", "address", "reason")
	SSHTCPAddressInvalid    = define(Error, "ssh_tcp_address_invalid", "Address Meridian doesn't ask a host to connect to", "value", "port")
)

// The meridian command's directories. mode is a file's or directory's
// permissions in octal, as 0755.
var (
	DirectoryVariableRelative = defineCommand("directory_variable_relative", "Directory variable isn't an absolute path", "variable", "value")
	HomeUnknown               = defineCommand("home_unknown", "No home directory", "variable")
	DirectoryUnavailable      = defineCommand("directory_unavailable", "Directory can't be created or opened", "path", "reason")
	NotADirectory             = defineCommand("not_a_directory", "Not a directory", "path")
	DirectoryNotOwned         = defineCommand("directory_not_owned", "Directory belongs to another user", "path")
	DirectoryNotPrivate       = defineCommand("directory_not_private", "Directory others can read or change", "path", "mode")
	PathUnsafe                = defineCommand("path_unsafe", "Path through a directory others can change", "path", "dir")
	NetworkFilesystem         = defineCommand("network_filesystem", "Database on a network filesystem", "path", "type")
	FilesystemUnknown         = defineCommand("filesystem_unknown", "Filesystem can't be told", "path", "reason")
	OwnerUnmapped             = defineCommand("owner_unmapped", "Owner hidden by a user namespace", "path", "dir")
	OwnerUnknown              = defineCommand("owner_unknown", "Owner can't be told", "path")
	DirectoryReplaced         = defineCommand("directory_replaced", "Directory replaced as it was made", "path")
	PathLinksTooMany          = defineCommand("path_links_too_many", "Path through too many symbolic links", "path", "max")
)

// The coordinator's store, a database in the state directory: path is its
// file; schema is which of its schemas, hopper's (hopper) or Meridian's own
// (meridian); and version and latest the version that schema is at, and
// the latest this release knows.
var (
	DatabaseTooNew = defineCommand("database_too_new", "Database newer than this release reads", "path", "schema", "version", "latest")
	DatabaseTooOld = defineCommand("database_too_old", "Database older than this release reads without changing it", "path", "schema", "version", "latest")
)

// The files the meridian command reads: its config, its key, a variable's
// value, a file of inputs and a workflow.
var (
	FileUnreadable = defineCommand("file_unreadable", "File can't be read", "path", "reason")
	FileNotRegular = defineCommand("file_not_regular", "Not a regular file", "path")
	FileNotOwned   = defineCommand("file_not_owned", "File belongs to another user", "path")
	FileNotPrivate = defineCommand("file_not_private", "File others can read or change", "path", "mode")
	FileChangeable = defineCommand("file_changeable", "File others can change", "path", "mode")
	FileOverLimit  = defineCommand("file_over_limit", "File larger than Meridian reads", "path", "limit")
)

// The meridian command's config, groups.yaml, variables.yaml, sources.yaml
// and clusters.yaml, whose problems say where they are with ParamFile and
// ParamLine. want is mapping, list, number, boolean or text; keys are the
// keys allowed, as QuoteAll writes them; variable is a variable's name as
// the plan writes it, as org.region; server a GitLab server's host name
// and port; and cluster a cluster's name.
var (
	ConfigYAMLSyntax       = defineCommand("config_yaml_syntax", "Config file isn't valid YAML", "reason")
	ConfigExtraDocument    = defineCommand("config_extra_document", "Config file with more than one YAML document")
	ConfigNotMapping       = defineCommand("config_not_mapping", "Config file that isn't a mapping")
	ConfigKeyUnknown       = defineCommand("config_key_unknown", "Unknown key in a config file", "key", "keys")
	ConfigKeyRepeated      = defineCommand("config_key_repeated", "Key given twice in a config file", "key", "first_line")
	ConfigWrongType        = defineCommand("config_wrong_type", "Config value of the wrong type", "key", "want")
	GitLabServerInvalid    = defineCommand("gitlab_server_invalid", "Invalid GitLab server", "server")
	GroupNameInvalid       = defineCommand("group_name_invalid", "Invalid group name", "group")
	GroupEmpty             = defineCommand("group_empty", "Group without members", "group")
	MemberInvalid          = defineCommand("member_invalid", "Invalid member", "group", "key", "value")
	MemberRepeated         = defineCommand("member_repeated", "Two members with the same name", "group", "member")
	MemberSettingsNotJSON  = defineCommand("member_settings_not_json", "Member's settings JSON can't hold", "group")
	VariableNameInvalid    = defineCommand("variable_name_invalid", "Invalid variable name", "context", "name")
	VariableNoValue        = defineCommand("variable_no_value", "Variable without a value", "variable")
	VariableFormInvalid    = defineCommand("variable_form_invalid", "Variable that's neither text nor a mapping", "variable")
	VariableSources        = defineCommand("variable_sources", "Variable without exactly one of value, env and file", "variable")
	VariableSecretInline   = defineCommand("variable_secret_inline", "Secret variable that holds its value", "variable")
	VariableEnvUnset       = defineCommand("variable_env_unset", "Variable's environment variable isn't set", "variable", "env")
	VariableFileUnreadable = defineCommand("variable_file_unreadable", "Variable's file can't be read", "variable", "file", "reason")
	VariableFileHome       = defineCommand("variable_file_home", "Variable's file in an unknown home directory", "variable", "file")
	ClusterNameInvalid     = defineCommand("cluster_name_invalid", "Invalid cluster name", "cluster")
	ClusterInvalid         = defineCommand("cluster_invalid", "Invalid cluster", "cluster", "key", "value")
	// A mapping of sources.yaml's workflows: key is its key, as it is
	// written; first the key of an earlier one that names the same
	// workflow, and first_line its line; and what is what of the workflow
	// in Git it maps to isn't one a step's uses: can name: uses, yaml or
	// host, server for a GitLab server gitlab-servers doesn't register,
	// platform for another workflow a platform stores, or template for a
	// version template where none can be.
	WorkflowMappingKeyInvalid = defineCommand("workflow_mapping_key_invalid", "Mapping of something other than a workflow a platform stores", "key")
	WorkflowMappingRepeated   = defineCommand("workflow_mapping_repeated", "Two mappings of one workflow", "key", "first", "first_line")
	WorkflowMappingInvalid    = defineCommand("workflow_mapping_invalid", "Mapping to something other than a workflow in Git", "key", "what")
)

// The key that wraps runs' data keys, and git, which reads the workflows
// uses: names.
var (
	KeyFileInvalid     = defineCommand("key_file_invalid", "Key file that isn't a key", "path", "size")
	KeyFileMissing     = defineCommand("key_file_missing", "Key file that isn't there", "path")
	KeyUnwrapFailed    = defineCommand("key_unwrap_failed", "Data key that can't be unwrapped", "run", "path")
	KeyWrappingUnknown = defineCommand("key_wrapping_unknown", "Data key wrapped an unknown way", "run")
	GitUnavailable     = defineCommand("git_unavailable", "git can't be run", "reason")
)

// What no meridian command, the worker above all, runs without: mode is
// how the process's cryptography runs, as GODEBUG's fips140 says: on, FIPS
// 140-3 mode without enforcing it, or off; and version the FIPS 140-3
// module's, latest for one compiled from Go's source.
var (
	FIPSNotEnforced        = defineCommand("fips_not_enforced", "Cryptography without FIPS 140-3 enforced", "mode")
	FIPSModuleNotValidated = defineCommand("fips_module_not_validated", "FIPS 140-3 module that isn't the validated one", "version")
)

// The meridian command's arguments, and what stops it. command is a
// command's name, as validate, and flag an option's, without its dashes.
var (
	CommandMissing    = defineCommand("command_missing", "No command")
	CommandUnknown    = defineCommand("command_unknown", "Unknown command", "command", "suggested", "suggestion")
	CommandNotBuilt   = defineCommand("command_not_built", "Command not built yet", "command")
	ArgumentsInvalid  = defineCommand("arguments_invalid", "Arguments a command doesn't take", "command")
	FlagUnknown       = defineCommand("flag_unknown", "Unknown option", "command", "flag")
	FlagNeedsValue    = defineCommand("flag_needs_value", "Option without its value", "command", "flag")
	FlagValueInvalid  = defineCommand("flag_value_invalid", "Option's value not one it takes", "command", "flag", "value")
	FlagSyntax        = defineCommand("flag_syntax", "Argument that isn't an option", "command", "argument")
	InputPathInvalid  = defineCommand("input_path_invalid", "Input path that isn't names joined with dots", "path")
	InputPathConflict = defineCommand("input_path_conflict", "Input path through a value", "path")
	// The -input options that validate can't convert by their inputs'
	// types: position is an -input's place among them, from 1; reason,
	// shape and kind say what is wrong, and next is the name a path goes
	// on to.
	InputFlagUnsplit         = defineCommand("input_flag_unsplit", "Input option without =", "position")
	InputFlagWrongType       = defineCommand("input_flag_wrong_type", "Input option's value not of its input's type", "path", "input", "type", "reason")
	InputFlagNotJSON         = defineCommand("input_flag_not_json", "Input option's value that isn't JSON", "path", "input", "type", "shape", "reason", "offset")
	InputFlagWrongShape      = defineCommand("input_flag_wrong_shape", "Input option's value of the wrong shape", "path", "input", "type", "shape", "kind")
	InputFlagPathNotSettable = defineCommand("input_flag_path_not_settable", "Input option's path not one it sets", "path", "input", "type", "reason", "next")
	InputsFileInvalid        = defineCommand("inputs_file_invalid", "File of inputs that isn't JSON", "reason")
	InputsFileNotObject      = defineCommand("inputs_file_not_object", "File of inputs that isn't an object")
	InputsFileExtra          = defineCommand("inputs_file_extra", "File of inputs with more than one value")
	InputsNumberTooLarge     = defineCommand("inputs_number_too_large", "Number too large", "number")
	Interrupted              = defineCommand("interrupted", "Interrupted")
	UnexpectedError          = defineCommand("unexpected_error", "Unexpected error", "reason")
)

// meridian run's own, and resume's: path is a run's directory, or the
// coordinator's socket; reason what the operating system said; limit the
// most bytes a socket's path can have on the system; and os the operating
// system's name, as Go's GOOS has it.
var (
	RunDirExists                 = defineCommand("run_dir_exists", "Run directory already there", "path")
	RunDirNotMade                = defineCommand("run_dir_not_made", "Run directory that can't be made", "path", "reason")
	SocketPathTooLong            = defineCommand("socket_path_too_long", "Socket path too long", "path", "limit")
	SocketUnusable               = defineCommand("socket_unusable", "Coordinator socket that can't be listened on", "path", "reason")
	SocketServerStopped          = defineCommand("socket_server_stopped", "Coordinator socket whose server stopped", "path")
	CoordinatorHardenFailed      = defineCommand("coordinator_harden_failed", "Coordinator's memory can't be protected", "reason")
	CoordinatorSystemUnsupported = defineCommand("coordinator_system_unsupported", "System where the coordinator can't protect its memory", "os")
)

// The problems of the commands that take a run that is stored, as
// meridian status, logs and resume do: run is the run's ID as it was
// given, and job a job's name or key.
var (
	RunNotFound = defineCommand("run_not_found", "Run that isn't stored", "run")
	JobNotFound = defineCommand("job_not_found", "Job the run doesn't have", "run", "job")
)

// meridian worker's own problems, which end it: variable is an environment
// variable's name, address the coordinator's, uid a user's number, os the
// operating system's name, as Go's GOOS has it, and reason what the
// operating system said; path is a socket's, or a worker API request's;
// status an HTTP status's number; at a JSON Pointer into an answer, which
// has_at says it has; data is events or log, and from the first event's
// number or byte of what the coordinator lost.
var (
	WorkerEnvironmentMissing    = defineCommand("worker_environment_missing", "Worker started without what it claims", "variable")
	WorkerEnvironmentInvalid    = defineCommand("worker_environment_invalid", "Worker started with what it can't use", "variable")
	CoordinatorPeerRefused      = defineCommand("coordinator_peer_refused", "Coordinator's socket served by another user", "path", "uid")
	CoordinatorPeerUnknown      = defineCommand("coordinator_peer_unknown", "Coordinator socket's user can't be told", "path", "reason")
	CoordinatorKeyMismatch      = defineCommand("coordinator_key_mismatch", "Server without the coordinator's key", "address")
	CoordinatorAnswerInvalid    = defineCommand("coordinator_answer_invalid", "Coordinator's answer the worker can't read", "path", "status", "has_at", "at")
	CoordinatorLostData         = defineCommand("coordinator_lost_data", "Coordinator lost what it had stored", "data", "from")
	CoordinatorStoredUnsent     = defineCommand("coordinator_stored_unsent", "Coordinator stored events the worker didn't send", "seq", "last")
	BootstrapTokenRefused       = defineCommand("bootstrap_token_refused", "Bootstrap token refused")
	BootstrapTokenInEnvironment = defineCommand("bootstrap_token_in_environment", "Bootstrap token in an environment others can read")
	RunPlanTooNew               = defineCommand("run_plan_too_new", "Run's plan newer than the worker reads", "version")
	AttemptSuperseded           = defineCommand("attempt_superseded", "Attempt ended without the worker")
	AttemptLeaseLapsed          = defineCommand("attempt_lease_lapsed", "Attempt's lease about to lapse")
	WorkerHardenFailed          = defineCommand("worker_harden_failed", "Worker's memory can't be protected", "reason")
	WorkerSystemUnsupported     = defineCommand("worker_system_unsupported", "System where the worker can't protect its memory", "os")
)

// What a session's port answers a browser with, in CommandRegistry, for a
// request it doesn't pass on to the session's service: name is the
// session's name; run and key the run's ID and the session's key in its
// sessions:, with which meridian session url makes a link to it; path
// the path the port serves the session at, as /me/session/<user>/<name>/;
// host the host the request asked for, as its Host header gives it; and
// limit how many connections to its service a session has open or
// opening at once, at most.
var (
	SessionUnreachable  = defineCommand("session_unreachable", "Session's service not answering", "name")
	SessionBusy         = defineCommand("session_busy", "Session with as many connections to its service as it may have", "name", "limit")
	SessionUnauthorized = defineCommand("session_unauthorized", "Session asked for without a link of Meridian's", "run", "key")
	SessionCrossOrigin  = defineCommand("session_cross_origin", "Request to a session from another page")
	SessionNotHere      = defineCommand("session_not_here", "No session at this path", "path")
	SessionHostRefused  = defineCommand("session_host_refused", "Session asked for at another host name", "host")
	// What meridian session finds of a run's session: run is the run's
	// ID, key the session's key, keys the keys of the run's sessions that
	// haven't ended, as QuoteAll writes them, and listed whether there are
	// any, "true" or "false"; root is the ID of the run at the top of its
	// tree.
	SessionKeyUnknown = defineCommand("session_key_unknown", "Run without such a session", "run", "key", "keys", "listed")
	SessionNotReady   = defineCommand("session_not_ready", "Session not served yet", "run", "key")
	SessionNotServed  = defineCommand("session_not_served", "Session no process serves", "run", "key", "root")
)

// The worker API's own problems, in WorkerRegistry, with which a coordinator
// refuses a worker's request, each 409 Conflict (workerapi): expected is the
// sequence number of the event, or the byte offset in a log, it expects
// next; placed and claimed are the digests, as sha256: and hex, of the
// worker binary an attempt's placement started and of the one that
// claimed it; version is the newest version of the worker API a worker
// speaks, and needed the one an attempt needs.
var (
	Fenced               = defineWorker("fenced", "Report from a superseded attempt")
	OutOfOrder           = defineWorker("out_of_order", "Report out of order", "expected")
	PlanTooNew           = defineWorker("plan_too_new", "Plan newer than the worker reads")
	WorkerAPITooOld      = defineWorker("worker_api_too_old", "Worker API older than the attempt needs", "version", "needed")
	WorkerDigestMismatch = defineWorker("worker_digest_mismatch", "Worker that isn't the binary placed", "placed", "claimed")
)
