package app

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	codeQLCommit = "7211b7c8077ea37d8641b6271f6a365a22a5fbfa"
	trivyCommit  = "ed142fd0673e97e23eac54620cfb913e5ce36c25"
)

type policyWorkflow struct {
	name     string
	contents string
	document *yaml.Node
}

func TestCISupplyChainPolicy(t *testing.T) {
	repositoryRoot := filepath.Join("..", "..", "..")
	t.Run("pre-commit revisions", func(t *testing.T) {
		precommit := readRepositoryPolicyFile(t, filepath.Join(repositoryRoot, ".pre-commit-config.yaml"))
		revisionPattern := regexp.MustCompile(`(?m)^\s*rev:\s*([0-9a-f]{40})(?:\s+#.*)?$`)
		if got := len(revisionPattern.FindAllStringSubmatch(precommit, -1)); got != 3 {
			t.Fatalf("pre-commit hooks must use three reviewed immutable revisions, found %d", got)
		}
		if regexp.MustCompile(`(?m)^\s*rev:\s*v`).MatchString(precommit) {
			t.Fatal("pre-commit hook tag is mutable")
		}
	})

	workflows := enumeratePolicyWorkflows(t, repositoryRoot)
	if len(workflows) == 0 {
		t.Fatal("no GitHub workflows found")
	}
	for _, workflow := range workflows {
		workflow := workflow
		t.Run("workflow/"+workflow.name, func(t *testing.T) {
			assertWorkflowPolicy(t, workflow)
		})
	}

	t.Run("mandatory workflows", func(t *testing.T) {
		byName := make(map[string]policyWorkflow, len(workflows))
		for _, workflow := range workflows {
			byName[workflow.name] = workflow
		}
		for _, name := range []string{
			"contracts.yml", "container.yml", "gateway-postgres.yml", "go.yml",
			"release.yml", "security.yml", "stale.yml",
		} {
			if _, ok := byName[name]; !ok {
				t.Errorf("mandatory workflow %s is missing", name)
			}
		}
	})

	t.Run("gateway workflows", func(t *testing.T) { assertGatewayWorkflowPolicy(t, repositoryRoot) })
	t.Run("release graph", func(t *testing.T) { assertReleaseWorkflowPolicy(t, repositoryRoot) })
	t.Run("credential scan exceptions", func(t *testing.T) { assertCredentialScanPolicy(t, repositoryRoot) })
	t.Run("license distribution", func(t *testing.T) { assertLicenseDistributionPolicy(t, repositoryRoot) })
	t.Run("documents", func(t *testing.T) { assertRepositoryDocumentPolicy(t, repositoryRoot) })
}

func enumeratePolicyWorkflows(t *testing.T, repositoryRoot string) []policyWorkflow {
	t.Helper()
	workflowDirectory := filepath.Join(repositoryRoot, ".github", "workflows")
	var paths []string
	for _, pattern := range []string{"*.yml", "*.yaml"} {
		matches, err := filepath.Glob(filepath.Join(workflowDirectory, pattern))
		if err != nil {
			t.Fatalf("glob workflows: %v", err)
		}
		paths = append(paths, matches...)
	}
	sort.Strings(paths)
	workflows := make([]policyWorkflow, 0, len(paths))
	for _, path := range paths {
		contents := readRepositoryTextFile(t, path)
		document := parseRepositoryYAML(t, path, contents)
		workflows = append(workflows, policyWorkflow{filepath.Base(path), contents, document})
	}
	return workflows
}

func assertWorkflowPolicy(t *testing.T, workflow policyWorkflow) {
	t.Helper()
	root := yamlDocumentRoot(t, workflow.document)
	permissions := yamlMapValue(root, "permissions")
	if got := yamlMapScalar(permissions, "contents"); got != "read" {
		t.Fatalf("%s top-level contents permission = %q, want read", workflow.name, got)
	}
	if workflowTriggerEnabled(root, "pull_request_target") {
		t.Fatalf("%s must not use pull_request_target", workflow.name)
	}
	if workflowTriggerEnabled(root, "pull_request") && regexp.MustCompile(`(?i)\$\{\{\s*secrets\s*\.`).MatchString(workflow.contents) {
		t.Fatalf("%s exposes repository secrets to pull-request code", workflow.name)
	}
	if regexp.MustCompile(`(?m)go-version:\s*["']?1\.26["']?\s*(?:#.*)?$`).MatchString(workflow.contents) {
		t.Fatalf("%s uses an unpinned Go minor version", workflow.name)
	}

	shaPattern := regexp.MustCompile(`^[0-9a-f]{40}$`)
	versionPattern := regexp.MustCompile(`^v[0-9]+(?:\.[0-9]+){0,2}(?:\s|$)`)
	walkYAML(root, func(node *yaml.Node) {
		if node.Kind != yaml.MappingNode {
			return
		}
		if run := yamlMapValue(node, "run"); run != nil && strings.Contains(run.Value, "${{") {
			t.Errorf("%s run script at line %d interpolates an expression instead of passing it through env", workflow.name, run.Line)
		}
		uses := yamlMapValue(node, "uses")
		if uses == nil || uses.Kind != yaml.ScalarNode || strings.HasPrefix(uses.Value, "./") {
			return
		}
		separator := strings.LastIndex(uses.Value, "@")
		if separator <= 0 || !shaPattern.MatchString(uses.Value[separator+1:]) {
			t.Errorf("%s action %q is not pinned to an immutable full SHA", workflow.name, uses.Value)
			return
		}
		if !versionPattern.MatchString(workflowVersionComment(workflow.contents, uses.Line)) {
			t.Errorf("%s action %q needs a reviewed version comment beside its SHA", workflow.name, uses.Value[:separator])
		}
		if strings.HasPrefix(uses.Value, "github/codeql-action/") && uses.Value[separator+1:] != codeQLCommit {
			t.Errorf("%s CodeQL action uses tag object or unreviewed commit %s; want peeled %s", workflow.name, uses.Value[separator+1:], codeQLCommit)
		}
		if strings.HasPrefix(uses.Value, "aquasecurity/trivy-action") && uses.Value[separator+1:] != trivyCommit {
			t.Errorf("%s Trivy action uses tag object or unreviewed commit %s; want peeled %s", workflow.name, uses.Value[separator+1:], trivyCommit)
		}
		if strings.HasPrefix(uses.Value, "actions/checkout@") {
			with := yamlMapValue(node, "with")
			if with == nil || yamlMapScalar(with, "persist-credentials") != "false" {
				t.Errorf("%s checkout at line %d must set persist-credentials: false", workflow.name, uses.Line)
			}
		}
	})

	jobs := yamlMapValue(root, "jobs")
	if jobs == nil || jobs.Kind != yaml.MappingNode {
		t.Fatalf("%s has no jobs mapping", workflow.name)
	}
	for index := 0; index < len(jobs.Content); index += 2 {
		jobName, job := jobs.Content[index].Value, jobs.Content[index+1]
		jobPermissions := yamlMapValue(job, "permissions")
		if yamlMapScalar(jobPermissions, "contents") == "write" && yamlMapScalar(jobPermissions, "id-token") == "write" {
			t.Errorf("%s job %s combines publication and signing authority", workflow.name, jobName)
		}
	}
	assertNoGitleaksBypassInWorkflow(t, workflow)
}

func assertNoGitleaksBypassInWorkflow(t *testing.T, workflow policyWorkflow) {
	t.Helper()
	configName := regexp.MustCompile(`(?i)gitleaks[\s_-]*config`)
	shortConfigFlag := regexp.MustCompile(`(?i)(?:^|\s)-c(?:\s|=)`)
	longConfigFlag := regexp.MustCompile(`(?i)(?:^|\s)--config(?:\s|=)`)
	allowMarker := strings.ToLower("gitleaks" + ":allow")
	walkYAML(yamlDocumentRoot(t, workflow.document), func(node *yaml.Node) {
		if node.Kind != yaml.ScalarNode {
			return
		}
		lower := strings.ToLower(node.Value)
		if configName.MatchString(lower) {
			t.Errorf("%s line %d sets a repository-specific gitleaks configuration", workflow.name, node.Line)
		}
		if strings.Contains(lower, "gitleaks") && (longConfigFlag.MatchString(lower) || shortConfigFlag.MatchString(lower)) {
			t.Errorf("%s line %d passes a custom configuration to gitleaks", workflow.name, node.Line)
		}
		if strings.Contains(lower, allowMarker) {
			t.Errorf("%s line %d contains an inline secret-scan bypass marker", workflow.name, node.Line)
		}
	})
}

func assertGatewayWorkflowPolicy(t *testing.T, repositoryRoot string) {
	t.Helper()
	postgres := readRepositoryPolicyFile(t, filepath.Join(repositoryRoot, ".github", "workflows", "gateway-postgres.yml"))
	if !strings.Contains(postgres, "./internal/gateway/provider/gmessages") || !strings.Contains(postgres, "-run TestPostgresIntegration") {
		t.Error("PostgreSQL CI does not select the production gmessages composition integration tests")
	}
	goWorkflow := readRepositoryPolicyFile(t, filepath.Join(repositoryRoot, ".github", "workflows", "go.yml"))
	for _, required := range []string{
		"go install github.com/rhysd/actionlint/cmd/actionlint@v1.7.12", "actionlint",
		"go test ./internal/gateway/app -run '^TestCISupplyChainPolicy$'",
		"./scripts/test-publish-release",
	} {
		if !strings.Contains(goWorkflow, required) {
			t.Errorf("go.yml workflow-policy gate is missing %q", required)
		}
	}
	security := readRepositoryPolicyFile(t, filepath.Join(repositoryRoot, ".github", "workflows", "security.yml"))
	for _, required := range []string{
		"fetch-depth: 0", `gitleaks git --redact=100 --no-banner --ignore-gitleaks-allow --log-opts="--all" .`,
		"govulncheck ./cmd/sirenaix-gateway", "github/codeql-action/init@" + codeQLCommit,
		"build-mode: manual", "go build -trimpath ./cmd/sirenaix-gateway",
		"github/codeql-action/analyze@" + codeQLCommit, "./scripts/check-licenses",
	} {
		if !strings.Contains(security, required) {
			t.Errorf("security workflow is missing %q", required)
		}
	}
	container := readRepositoryPolicyFile(t, filepath.Join(repositoryRoot, ".github", "workflows", "container.yml"))
	for _, required := range []string{
		`- ".dockerignore"`, `- "Dockerfile"`, `- "cmd/**"`, `- "internal/**"`, `- "pkg/**"`,
		`- "third_party/**"`, `- "scripts/check-licenses"`, `- "scripts/generate-third-party-licenses"`,
		`- "scripts/verify-container"`, `- "go.mod"`, `- "go.sum"`, `- "LICENSE"`,
		`- "LICENSE.exceptions"`, `- "NOTICE.md"`, "docker build --pull --file Dockerfile",
		"./scripts/verify-container", "aquasecurity/trivy-action@" + trivyCommit,
		"format: cyclonedx", "severity: CRITICAL,HIGH", "actions/upload-artifact@",
	} {
		if !strings.Contains(container, required) {
			t.Errorf("container workflow is missing trigger or gate %q", required)
		}
	}
}

func assertReleaseWorkflowPolicy(t *testing.T, repositoryRoot string) {
	t.Helper()
	path := filepath.Join(repositoryRoot, ".github", "workflows", "release.yml")
	contents := readRepositoryPolicyFile(t, path)
	root := yamlDocumentRoot(t, parseRepositoryYAML(t, path, contents))
	jobs := yamlMapValue(root, "jobs")
	mandatoryGates := []string{
		"unit-race", "postgres-integration", "contracts", "workflow-policy",
		"secrets", "licenses", "vulnerabilities", "container",
	}
	for _, jobName := range append([]string{"build", "provenance", "publish"}, mandatoryGates...) {
		if yamlMapValue(jobs, jobName) == nil {
			t.Errorf("release workflow is missing mandatory job %q", jobName)
		}
	}
	if t.Failed() {
		return
	}
	jobRequirements := map[string][]string{
		"unit-race": {
			"go test -count=1", "go test -race -count=1",
		},
		"postgres-integration": {
			"postgres:17-alpine@sha256:", "SIRENAIX_POSTGRES_TEST_DSN", "-tags=postgres_integration", "TestPostgresIntegration",
		},
		"contracts": {
			"TestOpenAPI",
		},
		"workflow-policy": {
			"actionlint", "TestCISupplyChainPolicy", "test-publish-release",
		},
		"secrets": {
			"fetch-depth", `gitleaks git --redact=100 --no-banner --ignore-gitleaks-allow --log-opts="--all" .`,
		},
		"licenses": {
			"./scripts/check-licenses",
		},
		"vulnerabilities": {
			"govulncheck ./cmd/sirenaix-gateway",
		},
		"container": {
			"docker build --pull --file Dockerfile", "./scripts/verify-container", "aquasecurity/trivy-action@" + trivyCommit, "cyclonedx",
		},
	}
	for jobName, requirements := range jobRequirements {
		job := yamlMapValue(jobs, jobName)
		for _, required := range requirements {
			if !nodeContainsScalar(job, required) {
				t.Errorf("release job %s is missing mandatory gate content %q", jobName, required)
			}
		}
	}
	build := yamlMapValue(jobs, "build")
	if got := yamlMapScalar(yamlMapValue(build, "permissions"), "contents"); got != "read" {
		t.Errorf("release build job contents permission = %q, want read", got)
	}
	provenance := yamlMapValue(jobs, "provenance")
	provenancePermissions := yamlMapValue(provenance, "permissions")
	if yamlMapScalar(provenancePermissions, "contents") != "read" || yamlMapScalar(provenancePermissions, "id-token") != "write" || yamlMapScalar(provenancePermissions, "attestations") != "write" {
		t.Error("release provenance permissions are not least-privilege")
	}
	publish := yamlMapValue(jobs, "publish")
	publishPermissions := yamlMapValue(publish, "permissions")
	if yamlMapScalar(publishPermissions, "contents") != "write" || yamlMapScalar(publishPermissions, "id-token") == "write" || yamlMapScalar(publishPermissions, "attestations") == "write" {
		t.Error("release publish permissions are not isolated")
	}
	if environment := yamlMapValue(publish, "environment"); yamlScalarOrMapName(environment) != "release" {
		t.Error("release publish job must use the protected release environment")
	}
	if nodeHasActionPrefix(publish, "actions/checkout@") {
		t.Error("release publish job must not need a checkout or Git working directory")
	}
	downloads := 0
	walkYAML(root, func(node *yaml.Node) {
		if node.Kind != yaml.MappingNode {
			return
		}
		uses := yamlMapValue(node, "uses")
		if uses == nil || !strings.HasPrefix(uses.Value, "actions/download-artifact@") {
			return
		}
		downloads++
		if got := yamlMapScalar(yamlMapValue(node, "with"), "path"); got != "." {
			t.Errorf("release artifact download path = %q, want repository-relative reconstruction at .", got)
		}
	})
	if downloads != 2 {
		t.Errorf("release workflow download-artifact step count = %d, want 2", downloads)
	}
	provenanceNeeds := yamlStringSet(yamlMapValue(provenance, "needs"))
	publishNeeds := yamlStringSet(yamlMapValue(publish, "needs"))
	for _, required := range append([]string{"build"}, mandatoryGates...) {
		if !provenanceNeeds[required] {
			t.Errorf("provenance must wait for release gate %q", required)
		}
		if !publishNeeds[required] {
			t.Errorf("publish must explicitly wait for release gate %q", required)
		}
	}
	if !publishNeeds["provenance"] {
		t.Error("publish must wait for signed provenance")
	}
	for _, required := range []string{
		"sha256sum --check checksums.txt", "actions/attest-build-provenance@",
		"sigstore/cosign-installer@", "cosign sign-blob --yes --bundle",
		"SIRENAIX_RELEASE_CONTROLS_ACKNOWLEDGED",
		"go install github.com/rhysd/actionlint/cmd/actionlint@v1.7.12", "actionlint",
		"./scripts/test-publish-release", "cp -R third_party", "cp scripts/publish-release release-tools/publish-release",
		"sh release-tools/publish-release dist",
	} {
		if !strings.Contains(contents, required) {
			t.Errorf("release workflow is missing %q", required)
		}
	}
	if strings.Contains(strings.ToLower(yamlMapScalar(publish, "name")), "immutable") {
		t.Error("publish job must not claim a release is immutable before repository controls enforce it")
	}
}

func assertCredentialScanPolicy(t *testing.T, repositoryRoot string) {
	t.Helper()
	expected := []string{
		"431a4c925e44a347fa695acb452dfe445fad39b2:pkg/connector/push.go:generic-api-key:57",
		"431a4c925e44a347fa695acb452dfe445fad39b2:pkg/libgm/util/constants.go:gcp-api-key:3",
	}
	lines := nonemptyLines(readRepositoryTextFile(t, filepath.Join(repositoryRoot, ".gitleaksignore")))
	if fmt.Sprint(lines) != fmt.Sprint(expected) {
		t.Fatalf(".gitleaksignore must contain only the two reviewed fingerprint exceptions; got %#v", lines)
	}
	allowMarker := []byte(strings.ToLower("gitleaks" + ":allow"))
	customConfigName := []byte(strings.ToLower(".gitleaks" + ".toml"))
	configEnvironment := []byte(strings.ToLower("GITLEAKS" + "_" + "CONFIG"))
	err := filepath.WalkDir(repositoryRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() && entry.Name() == ".git" {
			return filepath.SkipDir
		}
		if entry.IsDir() {
			return nil
		}
		if strings.EqualFold(entry.Name(), ".gitleaks"+".toml") {
			return fmt.Errorf("repository-specific gitleaks configuration is prohibited: %s", path)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > 10<<20 {
			return nil
		}
		contents, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lowerContents := bytes.ToLower(contents)
		if bytes.Contains(lowerContents, allowMarker) {
			return fmt.Errorf("inline secret-scan bypass marker is prohibited: %s", path)
		}
		if bytes.Contains(lowerContents, customConfigName) || bytes.Contains(lowerContents, configEnvironment) {
			return fmt.Errorf("secret scanner bypass configuration reference is prohibited: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func assertLicenseDistributionPolicy(t *testing.T, repositoryRoot string) {
	t.Helper()
	attributes := readRepositoryTextFile(t, filepath.Join(repositoryRoot, ".gitattributes"))
	if !strings.Contains(attributes, "third_party/licenses/** -text") {
		t.Error("generated license bundle must be committed without line-ending transformation")
	}
	for _, path := range []string{"third_party/README.md", "scripts/generate-third-party-licenses"} {
		if info, err := os.Stat(filepath.Join(repositoryRoot, filepath.FromSlash(path))); err != nil || info.IsDir() {
			t.Errorf("license distribution file %s is missing", path)
		}
	}
	entries, err := os.ReadDir(filepath.Join(repositoryRoot, "third_party", "licenses"))
	if err != nil || len(entries) == 0 {
		t.Errorf("committed third-party license bundle is missing or empty: %v", err)
	}
	moduleInventory := readRepositoryTextFile(t, filepath.Join(repositoryRoot, "third_party", "licenses", "MODULES.txt"))
	for _, required := range []string{"go.mau.fi/util v0.10.0", "github.com/aws/aws-sdk-go-v2 v1.43.7"} {
		if !strings.Contains(moduleInventory, required) {
			t.Errorf("compiled dependency inventory is missing %q", required)
		}
	}
	generator := readRepositoryTextFile(t, filepath.Join(repositoryRoot, "scripts", "generate-third-party-licenses"))
	for _, required := range []string{"GOOS=linux", "generate_bundle amd64", "generate_bundle arm64", "go-licenses save", "go list -deps", "MODULES.txt", "diff -ru"} {
		if !strings.Contains(generator, required) {
			t.Errorf("license generator is missing deterministic control %q", required)
		}
	}
	checker := readRepositoryTextFile(t, filepath.Join(repositoryRoot, "scripts", "check-licenses"))
	for _, required := range []string{"GOOS=linux", "GOARCH=", "--disallowed_types=forbidden,unknown", "generate-third-party-licenses", "third_party/licenses", "diff -ru"} {
		if !strings.Contains(checker, required) {
			t.Errorf("license checker is missing bundle audit %q", required)
		}
	}
	containerVerifier := readRepositoryTextFile(t, filepath.Join(repositoryRoot, "scripts", "verify-container"))
	if !strings.Contains(containerVerifier, "/usr/share/licenses/sirenaix-messaging-gateway") || !strings.Contains(containerVerifier, "third_party") {
		t.Error("container verifier does not byte-check the complete license distribution")
	}
}

func TestReleasePublisherDoesNotRequireGitWorkingTree(t *testing.T) {
	repositoryRoot := filepath.Join("..", "..", "..")
	publisher := readRepositoryTextFile(t, filepath.Join(repositoryRoot, "scripts", "publish-release"))
	for _, required := range []string{"gh release create", `--repo "$repository"`, "GITHUB_REPOSITORY", "GITHUB_REF_NAME"} {
		if !strings.Contains(publisher, required) {
			t.Errorf("release publisher is missing %q", required)
		}
	}
	if regexp.MustCompile(`(?m)(^|[;&|[:space:]])git([[:space:]]|$)`).MatchString(publisher) {
		t.Error("release publisher must not invoke git")
	}
	harness := readRepositoryTextFile(t, filepath.Join(repositoryRoot, "scripts", "test-publish-release"))
	for _, required := range []string{`cd "$temporary_root/work"`, "GITHUB_REPOSITORY=SirenaIX/test-gateway", "GITHUB_REF_NAME=v1.2.3"} {
		if !strings.Contains(harness, required) {
			t.Errorf("release publisher harness does not prove non-Git execution: missing %q", required)
		}
	}
}

func assertRepositoryDocumentPolicy(t *testing.T, repositoryRoot string) {
	t.Helper()
	for _, document := range []string{"NOTICE.md", "SECURITY.md", "CONTRIBUTING.md"} {
		if contents := readRepositoryTextFile(t, filepath.Join(repositoryRoot, document)); strings.TrimSpace(contents) == "" {
			t.Errorf("%s must not be empty", document)
		}
	}
	notice := readRepositoryTextFile(t, filepath.Join(repositoryRoot, "NOTICE.md"))
	for _, statement := range []string{
		"mautrix/gmessages", "v0.2608.0", "9743919f4884327db998fe0f227c073f3f3aceb3",
		"no special license exception", "not affiliated with, endorsed by, or sponsored by Google",
	} {
		if !strings.Contains(notice, statement) {
			t.Errorf("NOTICE.md is missing %q", statement)
		}
	}
	security := strings.Join(strings.Fields(strings.ToLower(readRepositoryTextFile(t, filepath.Join(repositoryRoot, "SECURITY.md")))), " ")
	for _, statement := range []string{
		"sirenaix_release_controls_acknowledged", "required reviewers", "immutable releases",
		"tag protection", "cannot verify those repository settings before publication",
	} {
		if !strings.Contains(security, statement) {
			t.Errorf("SECURITY.md is missing release prerequisite %q", statement)
		}
	}
	dependabot := readRepositoryPolicyFile(t, filepath.Join(repositoryRoot, ".github", "dependabot.yml"))
	for _, ecosystem := range []string{`package-ecosystem: "gomod"`, `package-ecosystem: "github-actions"`, `package-ecosystem: "docker"`} {
		if !strings.Contains(dependabot, ecosystem) {
			t.Errorf("dependabot is missing %s updates", ecosystem)
		}
	}
}

func yamlDocumentRoot(t *testing.T, document *yaml.Node) *yaml.Node {
	t.Helper()
	if document == nil || document.Kind != yaml.DocumentNode || len(document.Content) != 1 {
		t.Fatal("invalid YAML document")
	}
	return document.Content[0]
}

func yamlMapValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			return mapping.Content[index+1]
		}
	}
	return nil
}

func yamlMapScalar(mapping *yaml.Node, key string) string {
	value := yamlMapValue(mapping, key)
	if value == nil || value.Kind != yaml.ScalarNode {
		return ""
	}
	return value.Value
}

func yamlScalarOrMapName(node *yaml.Node) string {
	if node == nil {
		return ""
	}
	if node.Kind == yaml.ScalarNode {
		return node.Value
	}
	return yamlMapScalar(node, "name")
}

func walkYAML(node *yaml.Node, visit func(*yaml.Node)) {
	if node == nil {
		return
	}
	visit(node)
	for _, child := range node.Content {
		walkYAML(child, visit)
	}
}

func workflowTriggerEnabled(root *yaml.Node, trigger string) bool {
	on := yamlMapValue(root, "on")
	if on == nil {
		return false
	}
	switch on.Kind {
	case yaml.ScalarNode:
		return on.Value == trigger
	case yaml.SequenceNode:
		for _, item := range on.Content {
			if item.Value == trigger {
				return true
			}
		}
	case yaml.MappingNode:
		return yamlMapValue(on, trigger) != nil
	}
	return false
}

func yamlStringSet(node *yaml.Node) map[string]bool {
	values := make(map[string]bool)
	if node == nil {
		return values
	}
	if node.Kind == yaml.ScalarNode {
		values[node.Value] = true
	}
	if node.Kind == yaml.SequenceNode {
		for _, child := range node.Content {
			if child.Kind == yaml.ScalarNode {
				values[child.Value] = true
			}
		}
	}
	return values
}

func nodeHasActionPrefix(node *yaml.Node, prefix string) bool {
	found := false
	walkYAML(node, func(child *yaml.Node) {
		if child.Kind != yaml.MappingNode {
			return
		}
		uses := yamlMapValue(child, "uses")
		if uses != nil && strings.HasPrefix(uses.Value, prefix) {
			found = true
		}
	})
	return found
}

func nodeContainsScalar(node *yaml.Node, substring string) bool {
	found := false
	walkYAML(node, func(child *yaml.Node) {
		if child.Kind == yaml.ScalarNode && strings.Contains(child.Value, substring) {
			found = true
		}
	})
	return found
}

func nonemptyLines(contents string) []string {
	var lines []string
	for _, line := range strings.Split(contents, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func workflowVersionComment(contents string, lineNumber int) string {
	lines := strings.Split(contents, "\n")
	if lineNumber < 1 || lineNumber > len(lines) {
		return ""
	}
	line := lines[lineNumber-1]
	comment := strings.IndexByte(line, '#')
	if comment < 0 {
		return ""
	}
	return strings.TrimSpace(line[comment+1:])
}

func parseRepositoryYAML(t *testing.T, path, contents string) *yaml.Node {
	t.Helper()
	var document yaml.Node
	if err := yaml.Unmarshal([]byte(contents), &document); err != nil {
		t.Fatalf("parse %s as YAML: %v", path, err)
	}
	return &document
}

func readRepositoryPolicyFile(t *testing.T, path string) string {
	t.Helper()
	contents := readRepositoryTextFile(t, path)
	parseRepositoryYAML(t, path, contents)
	return contents
}

func readRepositoryTextFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return strings.ReplaceAll(string(contents), "\r\n", "\n")
}
