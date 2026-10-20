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
	NeedsUnknownKey            = define(Error, "needs_unknown_key", "Unknown key of needs", "job", "key")
	StepNotEarlier             = define(Error, "step_not_earlier", "Step doesn't run before this", "id")
	UndefinedStep              = define(Error, "undefined_step", "No such earlier step", "job", "id", "suggested", "suggestion")
	OutputUndefinedStep        = define(Warning, "output_undefined_step", "Output reads no such step", "job", "id", "suggested", "suggestion")
	StepsUnknownKey            = define(Error, "steps_unknown_key", "Unknown key of a step", "id", "key")
	OutputStepsUnknownKey      = define(Warning, "output_steps_unknown_key", "Output reads an unknown key of a step", "id", "key")
	NoMatrix                   = define(Warning, "no_matrix", "Job has no matrix", "job", "key", "text")
	UndefinedMatrixKey         = define(Warning, "undefined_matrix_key", "No such matrix key", "job", "key", "suggested", "suggestion")
	UndefinedSession           = define(Warning, "undefined_session", "No such session", "name", "suggested", "suggestion")
	ExpressionFailed           = define(Error, "expression_failed", "Expression failed", "reason")
	WordNotReference           = define(Warning, "word_not_reference", "Word that looks like a reference", "word", "text", "head")
	HyphenatedName             = define(Warning, "hyphenated_name", "Hyphenated name without brackets", "written", "word", "text", "name", "fix", "defines", "defined", "job")
	ConditionWord              = define(Warning, "condition_word", "Word that decides a condition", "word", "text")
	OlderAlways                = define(Warning, "older_always", "Older spelling of always()")
	ReplacedKeyword            = define(Warning, "replaced_keyword", "Replaced if: keyword", "word", "text", "replacement")
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
	ExpressionsReadEachOther = define(Error, "expressions_read_each_other", "Expressions read each other")
)

// Meridian's own limits on evaluating expressions, which aren't part of the
// format: expressions that are each within the language's limits can still
// pass them together.
var (
	ExpressionsBuildTooMuch = define(Error, "expressions_build_too_much", "Expressions build too much together", "limit")
)
