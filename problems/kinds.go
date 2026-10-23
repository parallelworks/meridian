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
	InputLooksSecret       = define(Warning, "input_looks_secret", "Input that looks like it holds a secret", "input")
	InputTooDeep           = define(Error, "input_too_deep", "Input's value nests too deeply", "input", "max")
	// ExecutionGroupUnknown is what the meridian command's Inputs gives
	// for an execution-groups input that names none of the user's groups:
	// value is the input's value, as Show writes it, and groups the groups
	// there are, as QuoteAll writes them.
	ExecutionGroupUnknown = define(Error, "execution_group_unknown", "No such execution group", "input", "value", "groups")
)

// The workflows a step's uses: names, as a Sources resolver reports them.
// ref is the uses: as it is written; repo a repository, as owner/name;
// location a workflow's place, as a plan's Source names it; commit, digest
// and pinned as Git and the plan write them; and reason what git said.
var (
	SourceRefInvalid       = define(Error, "source_ref_invalid", "Not a reference to a workflow in Git", "ref")
	SourceOnPlatform       = define(Error, "source_on_platform", "Workflow a platform stores", "ref")
	SourceRevisionNotFound = define(Error, "source_revision_not_found", "No such repository or revision", "repo", "revision", "reason")
	SourceFileNotFound     = define(Error, "source_file_not_found", "No such workflow file in the repository", "repo", "path", "commit")
	SourceTooLarge         = define(Error, "source_too_large", "Workflow file in a repository too large", "location", "commit", "limit")
	SourceChanged          = define(Error, "source_changed", "Workflow isn't the text it was pinned to", "location", "commit", "digest", "pinned")
	SourceNotPinned        = define(Error, "source_not_pinned", "Workflow not pinned", "location", "revision")
	SourceAccessDenied     = define(Error, "source_access_denied", "Access to a repository denied", "location", "reason")
	SourceObjectsInvalid   = define(Error, "source_objects_invalid", "Malformed Git objects", "location", "reason")
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
	JobUnreadable         = define(Error, "job_unreadable", "Job the worker couldn't read", "reason")
	RunDirUnusable        = define(Error, "run_dir_unusable", "Run directory the worker couldn't use", "path", "reason")
	EnvNameInvalid        = define(Error, "env_name_invalid", "Platform's variable with a name no variable can have", "name")
	UsesNotSupported      = define(Error, "uses_not_supported", "uses: step the worker can't run yet", "uses")
	StepSSHNotSupported   = define(Error, "step_ssh_not_supported", "Step on another host")
	StepNotStarted        = define(Error, "step_not_started", "Step's command not started", "reason")
	ShellNotSupported     = define(Error, "shell_not_supported", "Shell format 1 doesn't run", "shell")
	StepTimedOut          = define(Error, "step_timed_out", "Step ran past its timeout", "limit")
	OutputsTooLarge       = define(Error, "outputs_too_large", "Step's outputs too large", "size", "limit")
	OutputsLineTooLong    = define(Warning, "outputs_line_too_long", "Line of $OUTPUTS too long", "limit")
	OutputsNotFile        = define(Error, "outputs_not_file", "$OUTPUTS isn't a file")
	OutputsRemoved        = define(Warning, "outputs_removed", "$OUTPUTS removed as the step ran")
	LogNotKept            = define(Warning, "log_not_kept", "Log not kept whole", "size", "limit")
)

// The meridian command's own problems, in CommandRegistry. A path, an
// environment variable's name or value, or what the operating system said,
// reason, is a param as it is.

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
	PathLinksTooMany          = defineCommand("path_links_too_many", "Path through too many symbolic links", "path", "max")
	MountNotFound             = defineCommand("mount_not_found", "Mount not found", "path")
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

// The meridian command's config, groups.yaml and variables.yaml, whose
// problems say where they are with ParamFile and ParamLine. want is
// mapping, list, number, boolean or text; keys are the keys allowed, as
// QuoteAll writes them; and variable is a variable's name as the plan
// writes it, as org.region.
var (
	ConfigYAMLSyntax       = defineCommand("config_yaml_syntax", "Config file isn't valid YAML", "reason")
	ConfigExtraDocument    = defineCommand("config_extra_document", "Config file with more than one YAML document")
	ConfigNotMapping       = defineCommand("config_not_mapping", "Config file that isn't a mapping")
	ConfigKeyUnknown       = defineCommand("config_key_unknown", "Unknown key in a config file", "key", "keys")
	ConfigKeyRepeated      = defineCommand("config_key_repeated", "Key given twice in a config file", "key", "first_line")
	ConfigWrongType        = defineCommand("config_wrong_type", "Config value of the wrong type", "key", "want")
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
)

// The key that wraps runs' data keys, and git, which reads the workflows
// uses: names.
var (
	KeyFileInvalid     = defineCommand("key_file_invalid", "Key file that isn't a key", "path", "size")
	KeyUnwrapFailed    = defineCommand("key_unwrap_failed", "Data key that can't be unwrapped", "run", "path")
	KeyWrappingUnknown = defineCommand("key_wrapping_unknown", "Data key wrapped an unknown way", "run")
	GitUnavailable     = defineCommand("git_unavailable", "git can't be run", "reason")
)

// The meridian command's arguments, and what stops it. command is a
// command's name, as validate, and flag an option's, without its dashes.
var (
	CommandMissing       = defineCommand("command_missing", "No command")
	CommandUnknown       = defineCommand("command_unknown", "Unknown command", "command", "suggested", "suggestion")
	CommandNotBuilt      = defineCommand("command_not_built", "Command not built yet", "command")
	ArgumentsInvalid     = defineCommand("arguments_invalid", "Arguments a command doesn't take", "command")
	FlagUnknown          = defineCommand("flag_unknown", "Unknown option", "command", "flag")
	FlagNeedsValue       = defineCommand("flag_needs_value", "Option without its value", "command", "flag")
	FlagValueInvalid     = defineCommand("flag_value_invalid", "Option's value not one it takes", "command", "flag", "value")
	FlagSyntax           = defineCommand("flag_syntax", "Argument that isn't an option", "command", "argument")
	InputPathInvalid     = defineCommand("input_path_invalid", "Input path that isn't names joined with dots", "path")
	InputPathConflict    = defineCommand("input_path_conflict", "Input path through a value", "path")
	InputsFileInvalid    = defineCommand("inputs_file_invalid", "File of inputs that isn't JSON", "reason")
	InputsFileNotObject  = defineCommand("inputs_file_not_object", "File of inputs that isn't an object")
	InputsFileExtra      = defineCommand("inputs_file_extra", "File of inputs with more than one value")
	InputsNumberTooLarge = defineCommand("inputs_number_too_large", "Number too large", "number")
	Interrupted          = defineCommand("interrupted", "Interrupted")
	UnexpectedError      = defineCommand("unexpected_error", "Unexpected error", "reason")
)

// meridian worker's own problems, which end it: variable is an environment
// variable's name, address the coordinator's, and uid a user's number.
var (
	WorkerEnvironmentMissing = defineCommand("worker_environment_missing", "Worker started without what it claims", "variable")
	WorkerEnvironmentInvalid = defineCommand("worker_environment_invalid", "Worker started with what it can't use", "variable")
	CoordinatorPeerRefused   = defineCommand("coordinator_peer_refused", "Coordinator's socket served by another user", "path", "uid")
	CoordinatorKeyMismatch   = defineCommand("coordinator_key_mismatch", "Server without the coordinator's key", "address")
	BootstrapTokenRefused    = defineCommand("bootstrap_token_refused", "Bootstrap token refused")
	RunPlanTooNew            = defineCommand("run_plan_too_new", "Run's plan newer than the worker reads", "version")
	AttemptSuperseded        = defineCommand("attempt_superseded", "Attempt ended without the worker")
	AttemptLeaseLapsed       = defineCommand("attempt_lease_lapsed", "Attempt's lease about to lapse")
)
