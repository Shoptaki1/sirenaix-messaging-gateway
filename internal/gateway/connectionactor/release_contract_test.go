//go:build release_contract

package connectionactor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"

	"gopkg.in/yaml.v3"
)

const (
	expectedReleaseLoadCommand     = "go test -race -tags=loadtest -count=1 -v -timeout=9m -run '^TestSimulated1000Actors$' ./internal/gateway/connectionactor"
	expectedReleaseContractCommand = "go test -tags=release_contract -count=1 ./internal/gateway/connectionactor -run '^TestRelease'"
	expectedCheckoutAction         = "actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803"
	expectedSetupGoAction          = "actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16"
)

const validReleaseLoadContractFixture = `
env:
  GOTOOLCHAIN: local
jobs:
  load-race:
    runs-on: ubuntu-24.04
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803
        with:
          persist-credentials: false
      - uses: actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16
        with:
          go-version: "1.26.6"
          cache: true
      - run: >-
          go test -race -tags=loadtest -count=1 -v -timeout=9m
          -run '^TestSimulated1000Actors$'
          ./internal/gateway/connectionactor
  workflow-policy:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803
        with:
          persist-credentials: false
      - uses: actions/setup-go@924ae3a1cded613372ab5595356fb5720e22ba16
        with:
          go-version: "1.26.6"
          cache: true
      - run: >-
          go test -tags=release_contract -count=1
          ./internal/gateway/connectionactor -run '^TestRelease'
  provenance:
    needs:
      - build
      - load-race
  publish:
    needs: [provenance, load-race]
`

// TestReleaseWorkflowRequiresLoadRaceBeforePublication becomes an integration
// gate as soon as the release worktree is merged. This isolated branch does not
// contain release.yml, so absence is reported as a visible skip rather than a
// manufactured pass.
func TestReleaseWorkflowRequiresLoadRaceBeforePublication(t *testing.T) {
	root, err := findLoadRepositoryRoot()
	if err != nil {
		t.Fatal(err)
	}
	path := os.Getenv("SIRENAIX_RELEASE_WORKFLOW_PATH")
	if path == "" {
		path = filepath.Join(root, ".github", "workflows", "release.yml")
	}
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("release.yml is supplied by the separate release branch; final integration must rerun this contract")
	}
	if err != nil {
		t.Fatalf("read release workflow: %v", err)
	}
	if err := validateReleaseLoadContract(payload); err != nil {
		t.Fatalf("release workflow does not gate publication on the actor load race test: %v", err)
	}
}

func TestReleaseLoadContractRejectsMissingCommandOrPublicationDependency(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
		want    string
	}{
		{
			name:    "missing load job",
			fixture: strings.Replace(validReleaseLoadContractFixture, "load-race:", "load-check:", 1),
			want:    `job "load-race" is required`,
		},
		{
			name:    "missing load tag",
			fixture: strings.Replace(validReleaseLoadContractFixture, "-tags=loadtest ", "", 1),
			want:    "must run the loadtest build tag",
		},
		{
			name:    "command only echoes go test",
			fixture: strings.Replace(validReleaseLoadContractFixture, "go test ", "echo go test ", 1),
			want:    "must execute go test directly",
		},
		{
			name:    "wrong test selection",
			fixture: strings.Replace(validReleaseLoadContractFixture, "TestSimulated1000Actors", "TestSomethingElse", 1),
			want:    "must select exactly TestSimulated1000Actors",
		},
		{
			name:    "provenance bypasses load race",
			fixture: strings.Replace(validReleaseLoadContractFixture, "      - load-race\n  publish:", "  publish:", 1),
			want:    `job "provenance" must directly need "load-race"`,
		},
		{
			name:    "publish bypasses load race",
			fixture: strings.Replace(validReleaseLoadContractFixture, "needs: [provenance, load-race]", "needs: [provenance]", 1),
			want:    `job "publish" must directly need "load-race"`,
		},
		{
			name: "workflow policy omits contract invocation",
			fixture: strings.Replace(validReleaseLoadContractFixture,
				"go test -tags=release_contract -count=1\n          ./internal/gateway/connectionactor -run '^TestRelease'",
				"go test ./internal/gateway/app -run '^TestCISupplyChainPolicy$'", 1),
			want: `job "workflow-policy" must invoke the release contract exactly once`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateReleaseLoadContract([]byte(test.fixture))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validateReleaseLoadContract() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestReleaseLoadContractAcceptsExactRequiredGate(t *testing.T) {
	if err := validateReleaseLoadContract([]byte(validReleaseLoadContractFixture)); err != nil {
		t.Fatalf("validateReleaseLoadContract() error = %v", err)
	}
}

func TestReleaseLoadContractRejectsFailureMaskingMutations(t *testing.T) {
	mutateRun := func(suffix string) string {
		return strings.Replace(validReleaseLoadContractFixture,
			"./internal/gateway/connectionactor",
			"./internal/gateway/connectionactor "+suffix, 1)
	}
	mutateStep := func(field string) string {
		return strings.Replace(validReleaseLoadContractFixture,
			"      - run: >-",
			"      - "+field+"\n        run: >-", 1)
	}
	mutateJob := func(field string) string {
		return strings.Replace(validReleaseLoadContractFixture,
			"    timeout-minutes: 10",
			"    timeout-minutes: 10\n    "+field, 1)
	}
	literalNewline := strings.Replace(validReleaseLoadContractFixture, "      - run: >-", "      - run: |-", 1)
	literalNewline = strings.Replace(literalNewline,
		"          ./internal/gateway/connectionactor",
		"          ./internal/gateway/connectionactor\n          true", 1)
	tests := []struct {
		name    string
		fixture string
	}{
		{name: "or true", fixture: mutateRun("|| true")},
		{name: "and true", fixture: mutateRun("&& true")},
		{name: "pipe true", fixture: mutateRun("| true")},
		{name: "semicolon true", fixture: mutateRun("; true")},
		{name: "redirect output", fixture: mutateRun("> /dev/null")},
		{name: "trailing newline command", fixture: literalNewline},
		{name: "command substitution", fixture: mutateRun("$(true)")},
		{name: "backtick substitution", fixture: mutateRun("`true`")},
		{name: "step continue on error", fixture: mutateStep("continue-on-error: true")},
		{name: "step continue on error false", fixture: mutateStep("continue-on-error: false")},
		{name: "job continue on error", fixture: mutateJob("continue-on-error: true")},
		{name: "step false condition", fixture: mutateStep("if: false")},
		{name: "step always condition", fixture: mutateStep("if: always()")},
		{name: "job false condition", fixture: mutateJob("if: false")},
		{name: "step custom shell", fixture: mutateStep("shell: bash {0} || true")},
		{name: "job goflags environment", fixture: mutateJob("env:\n      GOFLAGS: -run=TestSomethingElse")},
		{name: "job run defaults", fixture: mutateJob("defaults:\n      run:\n        shell: bash {0} || true")},
		{name: "duplicate run", fixture: strings.Replace(validReleaseLoadContractFixture,
			"-run '^TestSimulated1000Actors$'", "-run '^TestSimulated1000Actors$' -run '^TestSomethingElse$'", 1)},
		{name: "duplicate count", fixture: strings.Replace(validReleaseLoadContractFixture,
			"-count=1", "-count=1 -count=0", 1)},
		{name: "duplicate tags", fixture: strings.Replace(validReleaseLoadContractFixture,
			"-tags=loadtest", "-tags=loadtest -tags=notloadtest", 1)},
		{name: "override race", fixture: strings.Replace(validReleaseLoadContractFixture,
			"go test -race", "go test -race -race=false", 1)},
		{name: "alternate package", fixture: mutateRun("./internal/gateway/domain")},
		{name: "extra setup command", fixture: strings.Replace(validReleaseLoadContractFixture,
			"      - run: >-", "      - run: echo 'GOFLAGS=-run=TestSomethingElse' >> \"$GITHUB_ENV\"\n      - run: >-", 1)},
		{name: "provenance always condition", fixture: strings.Replace(validReleaseLoadContractFixture,
			"  provenance:\n    needs:", "  provenance:\n    if: always()\n    needs:", 1)},
		{name: "publish continue on error", fixture: strings.Replace(validReleaseLoadContractFixture,
			"  publish:\n    needs:", "  publish:\n    continue-on-error: true\n    needs:", 1)},
		{name: "masked workflow policy invocation", fixture: strings.Replace(validReleaseLoadContractFixture,
			"./internal/gateway/connectionactor -run '^TestRelease'",
			"./internal/gateway/connectionactor -run '^TestRelease' || true", 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateReleaseLoadContract([]byte(test.fixture)); err == nil {
				t.Fatal("validateReleaseLoadContract() accepted a failure-masking mutation")
			}
		})
	}
}

type releaseLoadWorkflow struct {
	Env      map[string]string         `yaml:"env"`
	Defaults yaml.Node                 `yaml:"defaults"`
	Jobs     map[string]releaseLoadJob `yaml:"jobs"`
}

type releaseLoadJob struct {
	Needs           releaseNeeds      `yaml:"needs"`
	Steps           []releaseLoadStep `yaml:"steps"`
	TimeoutMinutes  int               `yaml:"timeout-minutes"`
	RunsOn          string            `yaml:"runs-on"`
	Uses            string            `yaml:"uses"`
	ContinueOnError yaml.Node         `yaml:"continue-on-error"`
	If              yaml.Node         `yaml:"if"`
	Env             yaml.Node         `yaml:"env"`
	Defaults        yaml.Node         `yaml:"defaults"`
	Strategy        yaml.Node         `yaml:"strategy"`
	Container       yaml.Node         `yaml:"container"`
	Services        yaml.Node         `yaml:"services"`
}

type releaseLoadStep struct {
	Name             string            `yaml:"name"`
	Run              string            `yaml:"run"`
	Uses             string            `yaml:"uses"`
	With             map[string]string `yaml:"with"`
	ContinueOnError  yaml.Node         `yaml:"continue-on-error"`
	If               yaml.Node         `yaml:"if"`
	Env              yaml.Node         `yaml:"env"`
	TimeoutMinutes   yaml.Node         `yaml:"timeout-minutes"`
	Shell            string            `yaml:"shell"`
	WorkingDirectory string            `yaml:"working-directory"`
}

type releaseNeeds []string

func (needs *releaseNeeds) UnmarshalYAML(node *yaml.Node) error {
	switch node.Kind {
	case yaml.ScalarNode:
		if node.Value != "" {
			*needs = append(*needs, node.Value)
		}
	case yaml.SequenceNode:
		for _, item := range node.Content {
			if item.Kind != yaml.ScalarNode || item.Value == "" {
				return errors.New("workflow needs entries must be non-empty job names")
			}
			*needs = append(*needs, item.Value)
		}
	case 0:
		return nil
	default:
		return errors.New("workflow needs must be a job name or list of job names")
	}
	return nil
}

func validateReleaseLoadContract(payload []byte) error {
	var workflow releaseLoadWorkflow
	if err := yaml.Unmarshal(payload, &workflow); err != nil {
		return fmt.Errorf("parse release workflow: %w", err)
	}
	if !exactStringMap(workflow.Env, map[string]string{"GOTOOLCHAIN": "local"}) {
		return errors.New(`release workflow env must contain only GOTOOLCHAIN: local`)
	}
	if yamlFieldPresent(workflow.Defaults) {
		return errors.New("release workflow must not override default run shell or working directory")
	}
	loadJob, ok := workflow.Jobs["load-race"]
	if !ok {
		return errors.New(`job "load-race" is required`)
	}
	if err := validateReleaseJobUnconditional("load-race", loadJob); err != nil {
		return err
	}
	if err := validateExactLoadJob(loadJob); err != nil {
		return err
	}
	policyJob, exists := workflow.Jobs["workflow-policy"]
	if !exists {
		return errors.New(`job "workflow-policy" is required`)
	}
	if err := validateReleaseJobUnconditional("workflow-policy", policyJob); err != nil {
		return err
	}
	if err := validateExactWorkflowPolicyContract(policyJob); err != nil {
		return err
	}
	for _, jobName := range []string{"provenance", "publish"} {
		job, exists := workflow.Jobs[jobName]
		if !exists {
			return fmt.Errorf("job %q is required", jobName)
		}
		if err := validateReleaseJobUnconditional(jobName, job); err != nil {
			return err
		}
		if !job.Needs.contains("load-race") {
			return fmt.Errorf("job %q must directly need %q", jobName, "load-race")
		}
	}
	return nil
}

func validateExactWorkflowPolicyContract(job releaseLoadJob) error {
	if job.RunsOn != "ubuntu-24.04" || job.Uses != "" || len(job.Needs) != 0 {
		return errors.New(`job "workflow-policy" must be an independent ubuntu-24.04 job`)
	}
	if yamlFieldPresent(job.Env) || yamlFieldPresent(job.Defaults) || yamlFieldPresent(job.Strategy) || yamlFieldPresent(job.Container) || yamlFieldPresent(job.Services) {
		return errors.New(`job "workflow-policy" must not override env, defaults, strategy, container, or services`)
	}
	if len(job.Steps) < 3 {
		return errors.New(`job "workflow-policy" must invoke the release contract exactly once after reviewed checkout and setup-go steps`)
	}
	if err := validateExactActionStep(job.Steps[0], expectedCheckoutAction, map[string]string{"persist-credentials": "false"}); err != nil {
		return fmt.Errorf("workflow-policy checkout step: %w", err)
	}
	if err := validateExactActionStep(job.Steps[1], expectedSetupGoAction, map[string]string{"go-version": "1.26.6", "cache": "true"}); err != nil {
		return fmt.Errorf("workflow-policy setup-go step: %w", err)
	}
	contractStep := job.Steps[2]
	if contractStep.Uses != "" || len(contractStep.With) != 0 {
		return errors.New(`job "workflow-policy" must invoke the release contract exactly once after reviewed checkout and setup-go steps`)
	}
	if err := validateLoadStepExecutionFields(contractStep); err != nil {
		return fmt.Errorf("workflow-policy release contract step: %w", err)
	}
	if err := validateNormalizedCommand(contractStep.Run, expectedReleaseContractCommand, "workflow-policy release contract command"); err != nil {
		return fmt.Errorf(`job "workflow-policy" must invoke the release contract exactly once: %w`, err)
	}
	for _, step := range job.Steps[3:] {
		if strings.Contains(step.Run, "release_contract") || strings.Contains(step.Run, "TestRelease") {
			return errors.New(`job "workflow-policy" must invoke the release contract exactly once`)
		}
	}
	return nil
}

func validateReleaseJobUnconditional(name string, job releaseLoadJob) error {
	if yamlFieldPresent(job.ContinueOnError) {
		return fmt.Errorf("job %q must not set continue-on-error", name)
	}
	if yamlFieldPresent(job.If) {
		return fmt.Errorf("job %q must not set if", name)
	}
	for index, step := range job.Steps {
		if yamlFieldPresent(step.ContinueOnError) {
			return fmt.Errorf("job %q step %d must not set continue-on-error", name, index+1)
		}
		if yamlFieldPresent(step.If) {
			return fmt.Errorf("job %q step %d must not set if", name, index+1)
		}
	}
	return nil
}

func validateExactLoadJob(job releaseLoadJob) error {
	if job.TimeoutMinutes != 10 {
		return fmt.Errorf(`job "load-race" timeout-minutes = %d, want 10`, job.TimeoutMinutes)
	}
	if job.RunsOn != "ubuntu-24.04" {
		return fmt.Errorf(`job "load-race" runs-on = %q, want ubuntu-24.04`, job.RunsOn)
	}
	if job.Uses != "" || len(job.Needs) != 0 {
		return errors.New(`job "load-race" must be an independent local job, not a reusable or dependent job`)
	}
	if yamlFieldPresent(job.Env) || yamlFieldPresent(job.Defaults) || yamlFieldPresent(job.Strategy) || yamlFieldPresent(job.Container) || yamlFieldPresent(job.Services) {
		return errors.New(`job "load-race" must not override env, defaults, strategy, container, or services`)
	}
	if len(job.Steps) != 3 {
		return fmt.Errorf(`job "load-race" step count = %d, want exact checkout, setup-go, and load test steps`, len(job.Steps))
	}
	if err := validateExactActionStep(job.Steps[0], expectedCheckoutAction, map[string]string{"persist-credentials": "false"}); err != nil {
		return fmt.Errorf("load-race checkout step: %w", err)
	}
	if err := validateExactActionStep(job.Steps[1], expectedSetupGoAction, map[string]string{"go-version": "1.26.6", "cache": "true"}); err != nil {
		return fmt.Errorf("load-race setup-go step: %w", err)
	}
	step := job.Steps[2]
	if step.Uses != "" || len(step.With) != 0 {
		return errors.New("load-race test step must be a direct run step")
	}
	if err := validateLoadStepExecutionFields(step); err != nil {
		return fmt.Errorf("load-race test step: %w", err)
	}
	command := step.Run
	if !loadCommandExecutesGoTest(command) {
		return errors.New(`job "load-race" must execute go test directly`)
	}
	if !loadCommandHasToken(command, "-race") {
		return errors.New(`job "load-race" must run go test with -race`)
	}
	if !loadCommandHasToken(command, "-tags=loadtest") {
		return errors.New(`job "load-race" must run the loadtest build tag`)
	}
	if !loadCommandSelectsExactTest(command) {
		return errors.New(`job "load-race" must select exactly TestSimulated1000Actors`)
	}
	if err := validateNormalizedCommand(command, expectedReleaseLoadCommand, "load-race command"); err != nil {
		return err
	}
	return nil
}

func validateExactActionStep(step releaseLoadStep, action string, with map[string]string) error {
	if step.Uses != action || step.Run != "" {
		return fmt.Errorf("must use exactly %s", action)
	}
	if !exactStringMap(step.With, with) {
		return fmt.Errorf("action %s inputs differ from the reviewed set", action)
	}
	return validateLoadStepExecutionFields(step)
}

func validateLoadStepExecutionFields(step releaseLoadStep) error {
	if yamlFieldPresent(step.Env) || yamlFieldPresent(step.TimeoutMinutes) || step.Shell != "" || step.WorkingDirectory != "" {
		return errors.New("must not override env, timeout-minutes, shell, or working-directory")
	}
	return nil
}

func validateNormalizedCommand(command, expected, label string) error {
	if strings.TrimSpace(command) != command {
		return fmt.Errorf("%s must not have leading or trailing whitespace", label)
	}
	for _, character := range command {
		if unicode.IsSpace(character) && character != ' ' {
			return fmt.Errorf("%s must be one physical command without control whitespace", label)
		}
	}
	got := strings.Fields(command)
	want := strings.Fields(expected)
	if len(got) != len(want) {
		return fmt.Errorf("%s has %d argv tokens, want exactly %d", label, len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			return fmt.Errorf("%s argv[%d] = %q, want %q", label, index, got[index], want[index])
		}
	}
	return nil
}

func exactStringMap(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		if got[key] != value {
			return false
		}
	}
	return true
}

func yamlFieldPresent(node yaml.Node) bool {
	return node.Kind != 0
}

func loadCommandExecutesGoTest(command string) bool {
	tokens := strings.Fields(command)
	return len(tokens) >= 2 && strings.Trim(tokens[0], "'\"") == "go" && strings.Trim(tokens[1], "'\"") == "test"
}

func loadCommandHasToken(command, expected string) bool {
	for _, token := range strings.Fields(command) {
		if strings.Trim(token, "'\"") == expected {
			return true
		}
	}
	return false
}

func loadCommandSelectsExactTest(command string) bool {
	tokens := strings.Fields(command)
	for index, token := range tokens {
		token = strings.Trim(token, "'\"")
		if token == "-run" && index+1 < len(tokens) {
			return strings.Trim(tokens[index+1], "'\"") == "^TestSimulated1000Actors$"
		}
		if strings.TrimPrefix(token, "-run=") == "^TestSimulated1000Actors$" {
			return true
		}
	}
	return false
}

func (needs releaseNeeds) contains(expected string) bool {
	for _, need := range needs {
		if need == expected {
			return true
		}
	}
	return false
}

func findLoadRepositoryRoot() (string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err = os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", errors.New("repository root containing go.mod not found")
		}
		directory = parent
	}
}
