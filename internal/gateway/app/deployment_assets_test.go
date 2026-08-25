package app

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestKubernetesBaseParsesAndSeparatesRuntimeMigrationAndOperationsBoundaries(t *testing.T) {
	base := filepath.Join("..", "..", "..", "deploy", "kubernetes", "base")
	kustomization := decodeYAMLMap(t, filepath.Join(base, "kustomization.yaml"))
	resourceValues := nestedSlice(t, kustomization, "resources")
	if len(resourceValues) < 6 {
		t.Fatalf("kustomization resources = %v", resourceValues)
	}
	kinds := make(map[string][]map[string]any)
	for _, value := range resourceValues {
		name, ok := value.(string)
		if !ok || filepath.Base(name) != name {
			t.Fatalf("unsafe kustomization resource %v", value)
		}
		document := decodeYAMLMap(t, filepath.Join(base, name))
		kind, _ := document["kind"].(string)
		if kind == "" || kind == "Secret" {
			t.Fatalf("resource %s has forbidden or empty kind %q", name, kind)
		}
		kinds[kind] = append(kinds[kind], document)
	}

	deployment := onlyKind(t, kinds, "Deployment")
	deploymentSpec := nestedMap(t, deployment, "spec")
	if replicas := asInt(t, deploymentSpec["replicas"]); replicas != 1 {
		t.Fatalf("pilot deployment replicas = %d, want exactly 1", replicas)
	}
	strategy := nestedMap(t, deploymentSpec, "strategy")
	if strategy["type"] != "Recreate" || strategy["rollingUpdate"] != nil {
		t.Fatalf("pilot deployment strategy can overlap process-local owners: %+v", strategy)
	}
	podSpec := nestedMap(t, nestedMap(t, nestedMap(t, deployment, "spec"), "template"), "spec")
	for _, unsupported := range []string{"topologySpreadConstraints", "affinity"} {
		if _, exists := podSpec[unsupported]; exists {
			t.Fatalf("pilot manifest makes unsupported scale-out claim with %s", unsupported)
		}
	}
	security := nestedMap(t, podSpec, "securityContext")
	if security["runAsNonRoot"] != true || asInt(t, security["runAsUser"]) != 65532 {
		t.Fatalf("pod security context = %+v", security)
	}
	if grace := asInt(t, podSpec["terminationGracePeriodSeconds"]); grace < 40 || grace > 120 {
		t.Fatalf("termination grace = %d, want a bounded interval covering coordinated shutdown", grace)
	}
	container := firstMap(t, nestedSlice(t, podSpec, "containers"))
	image, _ := container["image"].(string)
	if image != requiredKubernetesImage || strings.Contains(image, strings.Repeat("0", 64)) {
		t.Fatalf("deployment image = %q, want explicit required substitution", image)
	}
	containerSecurity := nestedMap(t, container, "securityContext")
	if containerSecurity["readOnlyRootFilesystem"] != true || containerSecurity["allowPrivilegeEscalation"] != false {
		t.Fatalf("container security context = %+v", containerSecurity)
	}
	capabilities := nestedMap(t, containerSecurity, "capabilities")
	if drops := nestedSlice(t, capabilities, "drop"); len(drops) != 1 || drops[0] != "ALL" {
		t.Fatalf("container capability drop = %v", drops)
	}
	if got := nestedString(t, nestedMap(t, container, "readinessProbe"), "httpGet", "path"); got != "/readyz" {
		t.Fatalf("readiness path = %q", got)
	}
	if got := nestedString(t, nestedMap(t, container, "livenessProbe"), "httpGet", "path"); got != "/livez" {
		t.Fatalf("liveness path = %q", got)
	}
	kmsSecretReference := false
	for _, variable := range nestedSlice(t, container, "env") {
		entry := variable.(map[string]any)
		if entry["name"] == "SIRENAIX_MIGRATION_DATABASE_URL" {
			t.Fatal("serve deployment received migration credentials")
		}
		if entry["name"] == "SIRENAIX_KMS_KEYS" {
			secretRef := nestedMap(t, nestedMap(t, entry, "valueFrom"), "secretKeyRef")
			kmsSecretReference = secretRef["name"] == "sirenaix-gateway-runtime" && secretRef["key"] == "kms-keys"
		}
	}
	if !kmsSecretReference {
		t.Fatal("serve deployment does not obtain KMS key identifiers from the runtime Secret")
	}
	if len(kinds["Job"]) != 0 || len(kinds["PodDisruptionBudget"]) != 0 {
		t.Fatalf("base includes migration or misleading HA asset: jobs=%d pdbs=%d", len(kinds["Job"]), len(kinds["PodDisruptionBudget"]))
	}

	networkPolicy := onlyKind(t, kinds, "NetworkPolicy")
	selector := nestedMap(t, nestedMap(t, networkPolicy, "spec"), "podSelector")
	matchLabels := nestedMap(t, selector, "matchLabels")
	if matchLabels["sirenaix.ai/network-restricted"] != "true" {
		t.Fatalf("network policy selector = %+v", matchLabels)
	}
	configMap := onlyKind(t, kinds, "ConfigMap")
	encoded, err := yaml.Marshal(configMap["data"])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"postgres://", "database-url", "BEGIN PRIVATE KEY", "aws-secret-access-key", "SIRENAIX_KMS_KEYS", "REDACTED", "gitleaks" + ":allow"} {
		if bytes.Contains(bytes.ToLower(encoded), bytes.ToLower([]byte(forbidden))) {
			t.Fatalf("ConfigMap contains secret-bearing value %q", forbidden)
		}
	}

	services := kinds["Service"]
	if len(services) != 2 {
		t.Fatalf("service count = %d, want public and internal ops", len(services))
	}
	for _, service := range services {
		metadata := nestedMap(t, service, "metadata")
		name, _ := metadata["name"].(string)
		ports := nestedSlice(t, nestedMap(t, service, "spec"), "ports")
		if name == "sirenaix-gateway" && firstMap(t, ports)["name"] != "https" {
			t.Fatalf("public service ports = %v", ports)
		}
		if name == "sirenaix-gateway-ops" && firstMap(t, ports)["name"] != "ops" {
			t.Fatalf("ops service ports = %v", ports)
		}
	}
}

const requiredKubernetesImage = "registry.invalid/sirenaix-gateway:REQUIRED_IMMUTABLE_RELEASE_IMAGE"

func TestKubernetesMigrationJobIsSeparateVersionedAndUsesTheDeploymentImage(t *testing.T) {
	root := filepath.Join("..", "..", "..", "deploy", "kubernetes")
	migration := filepath.Join(root, "migration")
	kustomization := decodeYAMLMap(t, filepath.Join(migration, "kustomization.yaml"))
	nameSuffix, _ := kustomization["nameSuffix"].(string)
	if nameSuffix != "-REQUIRED_RELEASE_ID" {
		t.Fatalf("migration nameSuffix = %q, want required per-release substitution", nameSuffix)
	}
	resources := nestedSlice(t, kustomization, "resources")
	if len(resources) != 1 || resources[0] != "job.yaml" {
		t.Fatalf("migration resources = %v", resources)
	}
	job := decodeYAMLMap(t, filepath.Join(migration, "job.yaml"))
	if job["kind"] != "Job" {
		t.Fatalf("migration resource kind = %v", job["kind"])
	}
	jobContainer := hardenedJobContainer(t, job)
	if jobContainer["image"] != requiredKubernetesImage {
		t.Fatalf("migration image = %v, want deployment image %q", jobContainer["image"], requiredKubernetesImage)
	}
	args := nestedSlice(t, jobContainer, "args")
	if len(args) != 2 || args[0] != "migrate" || args[1] != "up" {
		t.Fatalf("migration job args = %v", args)
	}
	for _, variable := range nestedSlice(t, jobContainer, "env") {
		entry := variable.(map[string]any)
		if entry["name"] == "SIRENAIX_DATABASE_URL" || entry["name"] == "SIRENAIX_ADMIN_DATABASE_URL" {
			t.Fatal("migration job received a runtime or tenant-admin database credential")
		}
	}
	for _, legacyPath := range []string{filepath.Join(root, "base", "migration-job.yaml"), filepath.Join(root, "base", "pdb.yaml")} {
		if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
			t.Fatalf("obsolete base asset still exists at %s", legacyPath)
		}
	}
	statusRoot := filepath.Join(root, "migration-status")
	statusKustomization := decodeYAMLMap(t, filepath.Join(statusRoot, "kustomization.yaml"))
	if statusKustomization["nameSuffix"] != "-REQUIRED_RELEASE_ID-status" {
		t.Fatalf("status nameSuffix = %v, want required per-release status job", statusKustomization["nameSuffix"])
	}
	statusJob := decodeYAMLMap(t, filepath.Join(statusRoot, "job.yaml"))
	statusContainer := hardenedJobContainer(t, statusJob)
	if statusContainer["image"] != requiredKubernetesImage {
		t.Fatalf("status image = %v, want migration image %q", statusContainer["image"], requiredKubernetesImage)
	}
	statusArgs := nestedSlice(t, statusContainer, "args")
	if len(statusArgs) != 3 || statusArgs[0] != "migrate" || statusArgs[1] != "status" || statusArgs[2] != "--check" {
		t.Fatalf("status job args = %v", statusArgs)
	}
}

func TestKubernetesMigrationBootstrapDeniesIngressAndAllowsOnlyDNSAndPostgresEgress(t *testing.T) {
	root := filepath.Join("..", "..", "..", "deploy", "kubernetes")
	bootstrap := filepath.Join(root, "bootstrap")
	kustomization := decodeYAMLMap(t, filepath.Join(bootstrap, "kustomization.yaml"))
	resources := nestedSlice(t, kustomization, "resources")
	if len(resources) != 1 || resources[0] != "networkpolicy.yaml" {
		t.Fatalf("bootstrap resources = %v, want one independently owned policy", resources)
	}
	policy := decodeYAMLMap(t, filepath.Join(bootstrap, "networkpolicy.yaml"))
	if policy["kind"] != "NetworkPolicy" || nestedMap(t, policy, "metadata")["name"] != "sirenaix-gateway-migration-jobs" {
		t.Fatalf("bootstrap policy identity = %+v", policy)
	}
	spec := nestedMap(t, policy, "spec")
	selectorLabels := nestedMap(t, nestedMap(t, spec, "podSelector"), "matchLabels")
	if selectorLabels["sirenaix.ai/migration-network-restricted"] != "true" {
		t.Fatalf("bootstrap selector = %+v", selectorLabels)
	}
	policyTypes := nestedSlice(t, spec, "policyTypes")
	if len(policyTypes) != 2 || policyTypes[0] != "Ingress" || policyTypes[1] != "Egress" {
		t.Fatalf("bootstrap policy types = %v", policyTypes)
	}
	if ingress := nestedSlice(t, spec, "ingress"); len(ingress) != 0 {
		t.Fatalf("migration/status policy permits ingress: %v", ingress)
	}
	egress := nestedSlice(t, spec, "egress")
	if len(egress) != 2 {
		t.Fatalf("migration/status egress = %v, want DNS and PostgreSQL only", egress)
	}
	dnsRule := egress[0].(map[string]any)
	dnsTargets := nestedSlice(t, dnsRule, "to")
	if len(dnsTargets) != 1 ||
		nestedMap(t, firstMap(t, dnsTargets), "namespaceSelector", "matchLabels")["kubernetes.io/metadata.name"] != "kube-system" {
		t.Fatalf("migration/status DNS target = %v, want kube-system only", dnsTargets)
	}
	allowedPorts := make(map[string]bool)
	for _, ruleValue := range egress {
		rule := ruleValue.(map[string]any)
		for _, portValue := range nestedSlice(t, rule, "ports") {
			port := portValue.(map[string]any)
			allowedPorts[port["protocol"].(string)+":"+asStringInt(t, port["port"])] = true
		}
	}
	for _, required := range []string{"UDP:53", "TCP:53", "TCP:5432"} {
		if !allowedPorts[required] {
			t.Fatalf("bootstrap egress ports = %v, missing %s", allowedPorts, required)
		}
	}
	if len(allowedPorts) != 3 {
		t.Fatalf("bootstrap egress exposes non-DNS/PostgreSQL ports: %v", allowedPorts)
	}

	for _, jobRoot := range []string{"migration", "migration-status"} {
		job := decodeYAMLMap(t, filepath.Join(root, jobRoot, "job.yaml"))
		labels := nestedMap(t, nestedMap(t, nestedMap(t, nestedMap(t, job, "spec"), "template"), "metadata"), "labels")
		if labels["sirenaix.ai/migration-network-restricted"] != "true" {
			t.Fatalf("%s Job is not selected by the bootstrap policy: %+v", jobRoot, labels)
		}
		if labels["sirenaix.ai/network-restricted"] == "true" {
			t.Fatalf("%s Job also matches the broader runtime policy, making NetworkPolicy egress additive", jobRoot)
		}
	}
}

func TestKubernetesOperatorCommandsUseSafeNameSuffixSeparatorAndApplyPolicyFirst(t *testing.T) {
	path := filepath.Join("..", "..", "..", "deploy", "README.md")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	document := string(contents)
	for _, command := range []string{
		`kustomize edit set namesuffix -- "-$RELEASE_ID"`,
		`kustomize edit set namesuffix -- "-$RELEASE_ID-status"`,
	} {
		if !strings.Contains(document, command) {
			t.Fatalf("operator guide lacks option-safe command %q", command)
		}
	}
	bootstrapApply := strings.Index(document, "kustomize build bootstrap | kubectl apply -f -")
	migrationApply := strings.Index(document, "kustomize build migration | kubectl apply -f -")
	statusApply := strings.Index(document, "kustomize build migration-status | kubectl apply -f -")
	if bootstrapApply < 0 || migrationApply < 0 || statusApply < 0 || bootstrapApply > migrationApply || bootstrapApply > statusApply {
		t.Fatalf("operator flow does not apply the migration NetworkPolicy before both Jobs")
	}
}

func TestKafkaAuthorizationDocumentationRequiresClusterDescribeAndIdempotentWrite(t *testing.T) {
	path := filepath.Join("..", "..", "..", "docs", "gateway-runtime-configuration.md")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	document := strings.Join(strings.Fields(string(contents)), " ")
	for _, requirement := range []string{
		"cluster `DESCRIBE` and `IDEMPOTENT_WRITE`",
		"`DescribeCluster` authorized-operations field itself requires cluster `DESCRIBE`",
	} {
		if !strings.Contains(document, requirement) {
			t.Fatalf("Kafka operator guidance lacks ACL requirement %q", requirement)
		}
	}
}

func hardenedJobContainer(t *testing.T, job map[string]any) map[string]any {
	t.Helper()
	pod := nestedMap(t, nestedMap(t, nestedMap(t, job, "spec"), "template"), "spec")
	if pod["restartPolicy"] != "Never" || pod["automountServiceAccountToken"] != false {
		t.Fatalf("job pod lifecycle/security = %+v", pod)
	}
	podSecurity := nestedMap(t, pod, "securityContext")
	if podSecurity["runAsNonRoot"] != true || asInt(t, podSecurity["runAsUser"]) != 65532 {
		t.Fatalf("job pod security context = %+v", podSecurity)
	}
	container := firstMap(t, nestedSlice(t, pod, "containers"))
	containerSecurity := nestedMap(t, container, "securityContext")
	if containerSecurity["readOnlyRootFilesystem"] != true || containerSecurity["allowPrivilegeEscalation"] != false {
		t.Fatalf("job container security context = %+v", containerSecurity)
	}
	drops := nestedSlice(t, nestedMap(t, containerSecurity, "capabilities"), "drop")
	if len(drops) != 1 || drops[0] != "ALL" {
		t.Fatalf("job capability drop = %v", drops)
	}
	return container
}

func TestComposeStackParsesAndUsesOneShotMigrationBeforeGateway(t *testing.T) {
	path := filepath.Join("..", "..", "..", "deploy", "compose", "compose.yaml")
	compose := decodeYAMLMap(t, path)
	services := nestedMap(t, compose, "services")
	for _, name := range []string{"postgres", "migrate", "tenant-init", "gateway"} {
		if _, ok := services[name].(map[string]any); !ok {
			t.Fatalf("compose service %q missing", name)
		}
	}
	migrate := services["migrate"].(map[string]any)
	command := nestedSlice(t, migrate, "command")
	if len(command) != 2 || command[0] != "migrate" || command[1] != "up" || migrate["read_only"] != true {
		t.Fatalf("migration service = %+v", migrate)
	}
	gateway := services["gateway"].(map[string]any)
	if gateway["read_only"] != true {
		t.Fatal("gateway compose service does not support a read-only root")
	}
	deploy := nestedMap(t, gateway, "deploy")
	if replicas := asInt(t, deploy["replicas"]); replicas != 1 {
		t.Fatalf("compose gateway replicas = %d, want exactly 1", replicas)
	}
	depends := nestedMap(t, gateway, "depends_on")
	tenantDependency := nestedMap(t, depends, "tenant-init")
	if tenantDependency["condition"] != "service_completed_successfully" {
		t.Fatalf("gateway dependency = %+v", tenantDependency)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(contents), "POSTGRES_PASSWORD: password") {
		t.Fatal("compose stack contains a default database password")
	}
}

func TestGatewayContainerContractIsPinnedNonRootAndOfflineVersionCapable(t *testing.T) {
	path := filepath.Join("..", "..", "..", "Dockerfile")
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	dockerfile := string(contents)
	firstLine := strings.SplitN(strings.ReplaceAll(dockerfile, "\r\n", "\n"), "\n", 2)[0]
	if !regexp.MustCompile(`^# syntax=docker/dockerfile:[0-9]+[.][0-9]+[.][0-9]+@sha256:[0-9a-f]{64}$`).MatchString(firstLine) {
		t.Fatalf("Dockerfile frontend is not immutably pinned: %q", firstLine)
	}
	lines := strings.Split(strings.ReplaceAll(dockerfile, "\r\n", "\n"), "\n")
	var from []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "FROM ") {
			from = append(from, trimmed)
		}
	}
	if len(from) != 2 || !strings.Contains(from[0], "@sha256:") || from[1] != "FROM scratch" {
		t.Fatalf("container stages = %v", from)
	}
	for _, required := range []string{
		`org.opencontainers.image.source=`, `org.opencontainers.image.version=`,
		`org.opencontainers.image.revision=`, `org.opencontainers.image.licenses="AGPL-3.0-or-later"`,
		`COPY LICENSE NOTICE.md /usr/share/licenses/sirenaix-messaging-gateway/`,
		`COPY LICENSE.exceptions /usr/share/licenses/sirenaix-messaging-gateway/LICENSE.exceptions`,
		`COPY third_party /usr/share/licenses/sirenaix-messaging-gateway/third_party/`,
		`USER 65532:65532`,
		`ENTRYPOINT ["/usr/local/bin/sirenaix-gateway"]`,
	} {
		if !strings.Contains(dockerfile, required) {
			t.Fatalf("Dockerfile missing runtime contract %q", required)
		}
	}
	if strings.Contains(dockerfile, "ENV SIRENAIX_") || strings.Contains(dockerfile, "CMD [\"serve\"]") {
		t.Fatal("container bakes runtime configuration or prevents the offline version probe")
	}
}

func TestLegacyContainerDefinitionsRetainRootContextBuildContract(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	ciBytes, err := os.ReadFile(filepath.Join(root, "Dockerfile.ci"))
	if err != nil {
		t.Fatal(err)
	}
	ci := string(ciBytes)
	if !strings.Contains(ci, `COPY ./packaging/legacy/docker-run.sh /docker-run.sh`) || !strings.Contains(ci, `COPY $EXECUTABLE /usr/bin/mautrix-gmessages`) {
		t.Fatalf("Dockerfile.ci no longer supports its root-context binary contract:\n%s", ci)
	}
	matrixBytes, err := os.ReadFile(filepath.Join(root, "packaging", "legacy", "Dockerfile.matrix-bridge"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(matrixBytes), `COPY . /build`) || !strings.Contains(string(matrixBytes), `RUN cp packaging/legacy/docker-run.sh /build/docker-run.sh`) {
		t.Fatalf("legacy source-build Dockerfile no longer uses the repository root context:\n%s", matrixBytes)
	}
	ignoreBytes, err := os.ReadFile(filepath.Join(root, ".dockerignore"))
	if err != nil {
		t.Fatal(err)
	}
	ignored := make(map[string]bool)
	for _, line := range strings.Split(strings.ReplaceAll(string(ignoreBytes), "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") && !strings.HasPrefix(line, "!") {
			ignored[line] = true
		}
	}
	for _, required := range []string{"Dockerfile.ci", "mautrix-gmessages", "packaging", "packaging/legacy/docker-run.sh"} {
		if ignored[required] {
			t.Fatalf("root-context legacy input %q is excluded by .dockerignore", required)
		}
	}
}

func decodeYAMLMap(t *testing.T, path string) map[string]any {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err = yaml.Unmarshal(contents, &value); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return value
}

func nestedMap(t *testing.T, value map[string]any, keys ...string) map[string]any {
	t.Helper()
	current := value
	for _, key := range keys {
		next, ok := current[key].(map[string]any)
		if !ok {
			t.Fatalf("%q is not a map in %+v", key, current)
		}
		current = next
	}
	return current
}

func nestedSlice(t *testing.T, value map[string]any, key string) []any {
	t.Helper()
	items, ok := value[key].([]any)
	if !ok {
		t.Fatalf("%q is not a list in %+v", key, value)
	}
	return items
}

func firstMap(t *testing.T, items []any) map[string]any {
	t.Helper()
	if len(items) == 0 {
		t.Fatal("expected a non-empty list")
	}
	value, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("first list value is not a map: %v", items[0])
	}
	return value
}

func onlyKind(t *testing.T, kinds map[string][]map[string]any, kind string) map[string]any {
	t.Helper()
	if len(kinds[kind]) != 1 {
		t.Fatalf("%s count = %d, want 1", kind, len(kinds[kind]))
	}
	return kinds[kind][0]
}

func nestedString(t *testing.T, value map[string]any, key, nestedKey string) string {
	t.Helper()
	nested := nestedMap(t, value, key)
	result, ok := nested[nestedKey].(string)
	if !ok {
		t.Fatalf("%s.%s is not a string", key, nestedKey)
	}
	return result
}

func asInt(t *testing.T, value any) int {
	t.Helper()
	switch typed := value.(type) {
	case int:
		return typed
	case uint64:
		return int(typed)
	default:
		t.Fatalf("value is not an integer: %T(%v)", value, value)
		return 0
	}
}

func asStringInt(t *testing.T, value any) string {
	t.Helper()
	switch typed := value.(type) {
	case int:
		return strconv.Itoa(typed)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case string:
		return typed
	default:
		t.Fatalf("value is not a string/integer: %T(%v)", value, value)
		return ""
	}
}
