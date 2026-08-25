package httpapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"go.mau.fi/mautrix-gmessages/internal/gateway/domain"
)

type documentedResponse struct {
	Ref     string         `yaml:"$ref"`
	Content map[string]any `yaml:"content"`
}

type documentedParameter struct {
	Ref      string         `yaml:"$ref"`
	Name     string         `yaml:"name"`
	Location string         `yaml:"in"`
	Required bool           `yaml:"required"`
	Schema   map[string]any `yaml:"schema"`
}

type documentedRequestBody struct {
	Content map[string]any `yaml:"content"`
}

type documentedOperation struct {
	Parameters  []documentedParameter         `yaml:"parameters"`
	RequestBody *documentedRequestBody        `yaml:"requestBody"`
	Responses   map[string]documentedResponse `yaml:"responses"`
}

type documentedPath struct {
	Parameters []documentedParameter `yaml:"parameters"`
	Get        *documentedOperation  `yaml:"get"`
	Post       *documentedOperation  `yaml:"post"`
	Patch      *documentedOperation  `yaml:"patch"`
	Put        *documentedOperation  `yaml:"put"`
	Delete     *documentedOperation  `yaml:"delete"`
}

type documentedAPI struct {
	OpenAPI    string                    `yaml:"openapi"`
	Paths      map[string]documentedPath `yaml:"paths"`
	Components struct {
		Responses  map[string]documentedResponse  `yaml:"responses"`
		Parameters map[string]documentedParameter `yaml:"parameters"`
		Schemas    map[string]map[string]any      `yaml:"schemas"`
	} `yaml:"components"`
}

func TestOpenAPIPairingIdentifierComponentSchemasMatchRuntimeLimits(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatalf("read OpenAPI document: %v", err)
	}
	var document documentedAPI
	if err := yaml.Unmarshal(contents, &document); err != nil {
		t.Fatalf("parse OpenAPI document: %v", err)
	}
	assertProperty := func(schemaName, property string, min, max int, pattern string) {
		t.Helper()
		schema := document.Components.Schemas[schemaName]
		properties, ok := schema["properties"].(map[string]any)
		if !ok {
			t.Fatalf("%s properties = %#v", schemaName, schema["properties"])
		}
		propertySchema, ok := properties[property].(map[string]any)
		if !ok {
			t.Fatalf("%s.%s = %#v", schemaName, property, properties[property])
		}
		if fmt.Sprint(propertySchema["minLength"]) != fmt.Sprint(min) || fmt.Sprint(propertySchema["maxLength"]) != fmt.Sprint(max) || fmt.Sprint(propertySchema["pattern"]) != pattern {
			t.Errorf("%s.%s schema = %#v", schemaName, property, propertySchema)
		}
	}
	for _, schemaName := range []string{"PairingDeviceSelectionRequest", "PairingIDRequest", "PairingAttempt"} {
		assertProperty(schemaName, "pairing_id", 8, 128, `^[A-Za-z0-9_-]{8,128}$`)
	}
	assertProperty("PairingDeviceSelectionRequest", "selected_device_id", 1, 256, `^[!-~]{1,256}$`)
}

func TestOpenAPITask7MessageMediaAndOneTimeSecretSchemasMatchRuntime(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatalf("read OpenAPI document: %v", err)
	}
	var document documentedAPI
	if err := yaml.Unmarshal(contents, &document); err != nil {
		t.Fatalf("parse OpenAPI document: %v", err)
	}

	message := document.Components.Schemas["Message"]
	required := fmt.Sprint(message["required"])
	for _, field := range []string{"id", "connection_id", "direction", "state", "text", "created_at"} {
		if !strings.Contains(required, field) {
			t.Errorf("Message.required = %s, missing %s", required, field)
		}
	}
	if strings.Contains(required, "conversation_id") {
		t.Errorf("Message.required = %s; new-chat queued messages need not have conversation_id yet", required)
	}
	messageProperties, ok := message["properties"].(map[string]any)
	if !ok {
		t.Fatalf("Message.properties = %#v", message["properties"])
	}
	for _, property := range []string{"provider_message_id", "transport", "attachments"} {
		if _, exists := messageProperties[property]; !exists {
			t.Errorf("Message is missing %s", property)
		}
	}
	directionSchema, ok := messageProperties["direction"].(map[string]any)
	if !ok || fmt.Sprint(directionSchema["enum"]) != "[inbound outbound unknown]" {
		t.Errorf("Message.direction does not expose truthful unknown provider direction: %#v", messageProperties["direction"])
	}

	attachment := document.Components.Schemas["MessageAttachment"]
	if fmt.Sprint(attachment["required"]) != "[media_id position]" {
		t.Errorf("MessageAttachment.required = %#v", attachment["required"])
	}
	secret := document.Components.Schemas["OneTimeWebhookSecret"]
	if secret["additionalProperties"] != false || !strings.Contains(fmt.Sprint(secret["required"]), "secret") {
		t.Errorf("OneTimeWebhookSecret = %#v", secret)
	}
	created := document.Components.Schemas["WebhookCreatedResponse"]
	if created["additionalProperties"] != false || fmt.Sprint(created["required"]) != "[endpoint secret]" {
		t.Errorf("WebhookCreatedResponse = %#v", created)
	}

	createResponse := document.Paths["/v1/webhooks"].Post.Responses["201"]
	rotateResponse := document.Paths["/v1/webhooks/{webhook_id}:rotate"].Post.Responses["200"]
	if got := responseSchemaRef(t, createResponse); got != "#/components/schemas/WebhookCreatedResponse" {
		t.Errorf("create webhook response ref = %q", got)
	}
	if got := responseSchemaRef(t, rotateResponse); got != "#/components/schemas/OneTimeWebhookSecret" {
		t.Errorf("rotate webhook response ref = %q", got)
	}

	mediaResponse := document.Paths["/v1/media/{media_id}/content"].Get.Responses["200"]
	for _, mediaType := range []string{"image/jpeg", "image/png", "image/gif", "image/webp"} {
		if _, exists := mediaResponse.Content[mediaType]; !exists {
			t.Errorf("media response is missing %s", mediaType)
		}
	}
}

func TestOpenAPITask8ServerContactAndLineCapabilitiesMatchRuntime(t *testing.T) {
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var document documentedAPI
	if err = yaml.Unmarshal(contents, &document); err != nil {
		t.Fatal(err)
	}
	put := document.Paths["/v1/contacts"].Put
	if put == nil || put.RequestBody == nil {
		t.Fatal("PUT /v1/contacts request is undocumented")
	}
	representation, ok := put.RequestBody.Content["application/json"].(map[string]any)
	if !ok {
		t.Fatalf("contact upsert representation = %#v", put.RequestBody.Content)
	}
	requestSchema, ok := representation["schema"].(map[string]any)
	if !ok || requestSchema["$ref"] != "#/components/schemas/ServerContactUpsert" {
		t.Fatalf("contact upsert schema = %#v", representation["schema"])
	}
	upsert := document.Components.Schemas["ServerContactUpsert"]
	if upsert["additionalProperties"] != false || fmt.Sprint(upsert["required"]) != "[phone]" {
		t.Fatalf("ServerContactUpsert = %#v", upsert)
	}
	upsertProperties := upsert["properties"].(map[string]any)
	phone := upsertProperties["phone"].(map[string]any)
	alias := upsertProperties["server_alias"].(map[string]any)
	if phone["pattern"] != `^\+[1-9][0-9]{1,14}$` || fmt.Sprint(phone["maxLength"]) != "16" ||
		alias["nullable"] != true || fmt.Sprint(alias["maxLength"]) != "256" {
		t.Fatalf("server contact field limits = phone %#v alias %#v", phone, alias)
	}

	line := document.Components.Schemas["Line"]
	required := fmt.Sprint(line["required"])
	for _, field := range []string{"rcs_enabled", "provider_sim_number", "provider_sim_payload_type", "discovery_source"} {
		if !strings.Contains(required, field) {
			t.Errorf("Line.required = %s, missing %s", required, field)
		}
	}
	lineProperties := line["properties"].(map[string]any)
	discovery := lineProperties["discovery_source"].(map[string]any)
	if fmt.Sprint(discovery["enum"]) != "[legacy_unknown authenticated_google_settings]" {
		t.Errorf("Line.discovery_source = %#v", discovery)
	}
	capabilities := document.Components.Schemas["LineRoutingCapabilities"]
	if fmt.Sprint(capabilities["required"]) != "[explicit_line_send new_conversation_line_selection new_conversation_route]" {
		t.Errorf("LineRoutingCapabilities = %#v", capabilities)
	}
}

func responseSchemaRef(t *testing.T, response documentedResponse) string {
	t.Helper()
	representation, ok := response.Content["application/json"].(map[string]any)
	if !ok {
		t.Fatalf("application/json representation = %#v", response.Content["application/json"])
	}
	schema, ok := representation["schema"].(map[string]any)
	if !ok {
		t.Fatalf("response schema = %#v", representation["schema"])
	}
	return fmt.Sprint(schema["$ref"])
}

type operationContract struct {
	statuses      []string
	requestJSON   bool
	query         []string
	binaryOK      bool
	binaryRequest bool
	skipBodyLimit bool
}

func TestOpenAPIAndImplementedRouteParity(t *testing.T) {
	expected := map[string]map[string]operationContract{
		"/v1/connections": {
			http.MethodGet:  {statuses: []string{"200", "400", "401", "403", "405", "413"}, query: []string{"after", "limit"}},
			http.MethodPost: {statuses: []string{"201", "400", "401", "403", "405", "409", "413", "415"}, requestJSON: true},
		},
		"/v1/connections/{connection_id}/health": {
			http.MethodGet: {statuses: []string{"200", "400", "401", "403", "404", "405", "413"}},
		},
		"/v1/connections/{connection_id}/lines": {
			http.MethodGet: {statuses: []string{"200", "400", "401", "403", "404", "405", "413"}},
		},
		"/v1/connections/{connection_id}/messages": {
			http.MethodPost: {statuses: []string{"202", "400", "401", "403", "404", "405", "409", "413", "415", "500"}, requestJSON: true},
		},
		"/v1/messages": {
			http.MethodGet: {statuses: []string{"200", "400", "401", "403", "405", "413", "500"}, query: []string{"after", "limit"}},
		},
		"/v1/messages/{message_id}": {
			http.MethodGet: {statuses: []string{"200", "400", "401", "403", "404", "405", "413", "500"}},
		},
		"/v1/media": {
			http.MethodPost: {statuses: []string{"201", "400", "401", "403", "405", "413", "415", "500"}, binaryRequest: true, skipBodyLimit: true},
		},
		"/v1/media/{media_id}": {
			http.MethodGet: {statuses: []string{"200", "400", "401", "403", "404", "405", "413", "500"}},
		},
		"/v1/media/{media_id}/content": {
			http.MethodGet: {statuses: []string{"200", "400", "401", "403", "404", "405", "413", "500"}, binaryOK: true},
		},
		"/v1/webhooks": {
			http.MethodGet:  {statuses: []string{"200", "400", "401", "403", "405", "413", "500"}, query: []string{"after", "limit"}},
			http.MethodPost: {statuses: []string{"201", "400", "401", "403", "405", "409", "413", "415", "500"}, requestJSON: true},
		},
		"/v1/webhooks/{webhook_id}": {
			http.MethodDelete: {statuses: []string{"202", "204", "400", "401", "403", "404", "405", "413", "500"}},
		},
		"/v1/webhooks/{webhook_id}:rotate": {
			http.MethodPost: {statuses: []string{"200", "400", "401", "403", "404", "405", "413", "500"}},
		},
		"/v1/webhooks/dlq/{dlq_id}:replay": {
			http.MethodPost: {statuses: []string{"202", "400", "401", "403", "404", "405", "413", "500"}},
		},
		"/v1/connections/{connection_id}/contacts:sync": {
			http.MethodPost: {statuses: []string{"200", "400", "401", "403", "404", "405", "413"}},
		},
		"/v1/connections/{connection_id}/pairing/start": {
			http.MethodPost: {statuses: []string{"200", "400", "401", "403", "404", "405", "409", "410", "413", "415", "500"}, requestJSON: true},
		},
		"/v1/connections/{connection_id}/pairing/complete": {
			http.MethodPost: {statuses: []string{"200", "400", "401", "403", "404", "405", "409", "410", "413", "415", "500"}, requestJSON: true},
		},
		"/v1/connections/{connection_id}/pairing/cancel": {
			http.MethodPost: {statuses: []string{"204", "400", "401", "403", "404", "405", "409", "410", "413", "415", "500"}, requestJSON: true},
		},
		"/v1/connections/{connection_id}/reauthorize": {
			http.MethodPost: {statuses: []string{"200", "400", "401", "403", "404", "405", "413", "415"}, requestJSON: true},
		},
		"/v1/contacts": {
			http.MethodGet: {statuses: []string{"200", "400", "401", "403", "405", "413"}, query: []string{"cursor", "limit"}},
			http.MethodPut: {statuses: []string{"200", "400", "401", "403", "405", "413", "415", "500"}, requestJSON: true},
		},
		"/v1/contacts/{contact_id}": {
			http.MethodPatch: {statuses: []string{"204", "400", "401", "403", "404", "405", "413", "415"}, requestJSON: true},
		},
		"/v1/labels": {
			http.MethodGet:  {statuses: []string{"200", "400", "401", "403", "405", "413"}},
			http.MethodPost: {statuses: []string{"200", "201", "400", "401", "403", "405", "413", "415"}, requestJSON: true},
		},
		"/v1/contacts/{contact_id}/labels/{label_id}": {
			http.MethodPut:    {statuses: []string{"204", "400", "401", "403", "404", "405", "413"}},
			http.MethodDelete: {statuses: []string{"204", "400", "401", "403", "404", "405", "413"}},
		},
	}
	contents, err := os.ReadFile("openapi.yaml")
	if err != nil {
		t.Fatalf("read OpenAPI document: %v", err)
	}
	var document documentedAPI
	var raw struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(contents, &document); err != nil {
		t.Fatalf("parse OpenAPI document: %v", err)
	}
	if err := yaml.Unmarshal(contents, &raw); err != nil {
		t.Fatalf("parse raw OpenAPI paths: %v", err)
	}
	if !strings.HasPrefix(document.OpenAPI, "3.") {
		t.Fatalf("OpenAPI version = %q", document.OpenAPI)
	}
	if len(document.Paths) != len(expected) {
		t.Fatalf("documented path count = %d, want %d", len(document.Paths), len(expected))
	}

	store := newFakeStore(t)
	store.contacts = []domain.Contact{{ID: "contact-example", TenantID: "tenant-example", Phone: mustPhone(t, "+12025550166")}}
	store.labels = []domain.Label{{ID: "label-example", TenantID: "tenant-example", Name: "Existing", Slug: "existing"}}
	handler := newTestHandler(t, store, &fakeSyncer{}, validVerifier())
	for path, methods := range expected {
		pathItem, ok := document.Paths[path]
		if !ok {
			t.Errorf("OpenAPI is missing path %s", path)
			continue
		}
		assertPathParameters(t, document, path, pathItem.Parameters)
		assertExactOperationSet(t, path, raw.Paths[path], methods)
		for method, contract := range methods {
			operation := operationFor(pathItem, method)
			if operation == nil {
				t.Errorf("OpenAPI is missing %s %s", method, path)
				continue
			}
			assertOperationContract(t, document, path, method, operation, contract)
			requestPath := strings.NewReplacer("{connection_id}", "connection-example", "{contact_id}", "contact-example", "{label_id}", "label-example", "{message_id}", "message-example", "{media_id}", "media-example", "{webhook_id}", "webhook-example", "{dlq_id}", "dlq-example").Replace(path)
			body := ""
			if method == http.MethodPatch {
				body = `{"server_alias":"Lead"}`
			} else if method == http.MethodPut && path == "/v1/contacts" {
				body = `{"phone":"+12025550166","server_alias":"Lead"}`
			} else if method == http.MethodPost && path == "/v1/labels" {
				body = `{"name":"Potential Client"}`
			}
			var responseStatus int
			if body == "" {
				request := authenticatedRequest(method, requestPath, nil, "valid")
				if path == "/v1/connections/{connection_id}/messages" {
					request.Header.Set("Idempotency-Key", "contract-idempotency")
				}
				responseStatus = serveRequest(handler, request).Code
			} else {
				responseStatus = serveJSON(handler, method, requestPath, body, "valid").Code
			}
			if responseStatus == http.StatusNotFound || responseStatus == http.StatusMethodNotAllowed {
				t.Errorf("implemented router does not represent %s %s (status %d)", method, path, responseStatus)
			}
			if contract.requestJSON {
				unsupportedRequest := authenticatedRequest(method, requestPath, strings.NewReader(`{}`), "valid")
				if path == "/v1/connections/{connection_id}/messages" {
					unsupportedRequest.Header.Set("Idempotency-Key", "contract-idempotency")
				}
				unsupported := serveRequest(handler, unsupportedRequest)
				assertError(t, unsupported, http.StatusUnsupportedMediaType, "unsupported_media_type", "")
			}
			if contract.skipBodyLimit {
				continue
			}
			overLimit := maxJSONBodyBytes + 1
			if path == "/v1/connections/{connection_id}/messages" {
				overLimit = maxMessageJSONBytes + 1
			}
			oversizedBody := strings.Repeat("x", overLimit)
			if contract.requestJSON {
				if method == http.MethodPatch {
					oversizedBody = `{"server_alias":"` + oversizedBody + `"}`
				} else {
					oversizedBody = `{"name":"` + oversizedBody + `"}`
				}
			}
			oversizedRequest := authenticatedRequest(method, requestPath, strings.NewReader(oversizedBody), "valid")
			if contract.requestJSON {
				oversizedRequest.Header.Set("Content-Type", "application/json")
			}
			if path == "/v1/connections/{connection_id}/messages" {
				oversizedRequest.Header.Set("Idempotency-Key", "contract-oversized")
			}
			oversizedResponse := httptest.NewRecorder()
			handler.ServeHTTP(oversizedResponse, oversizedRequest)
			assertError(t, oversizedResponse, http.StatusRequestEntityTooLarge, "request_too_large", "")
		}
		requestPath := strings.NewReplacer("{connection_id}", "connection-example", "{contact_id}", "contact-example", "{label_id}", "label-example", "{message_id}", "message-example", "{media_id}", "media-example", "{webhook_id}", "webhook-example", "{dlq_id}", "dlq-example").Replace(path)
		wrong := serve(handler, http.MethodOptions, requestPath, nil, "valid")
		assertError(t, wrong, http.StatusMethodNotAllowed, "method_not_allowed", "")
		if got, want := wrong.Header().Get("Allow"), joinedMethods(methods); got != want {
			t.Errorf("OPTIONS %s Allow = %q, want %q", path, got, want)
		}
	}
}

func assertExactOperationSet(t *testing.T, path string, raw map[string]any, expected map[string]operationContract) {
	t.Helper()
	actual := make([]string, 0)
	for key := range raw {
		upper := strings.ToUpper(key)
		switch upper {
		case http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodPut, http.MethodDelete, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodConnect:
			actual = append(actual, upper)
		}
	}
	want := make([]string, 0, len(expected))
	for method := range expected {
		want = append(want, method)
	}
	sort.Strings(actual)
	sort.Strings(want)
	if fmt.Sprint(actual) != fmt.Sprint(want) {
		t.Errorf("OpenAPI operations for %s = %v, want %v", path, actual, want)
	}
}

func assertPathParameters(t *testing.T, document documentedAPI, path string, parameters []documentedParameter) {
	t.Helper()
	want := make([]string, 0)
	for _, segment := range strings.Split(path, "/") {
		start, end := strings.Index(segment, "{"), strings.Index(segment, "}")
		if start >= 0 && end > start {
			want = append(want, segment[start+1:end])
		}
	}
	actual := make([]string, 0, len(parameters))
	for _, parameter := range parameters {
		parameter = resolveParameter(document, parameter)
		if parameter.Location != "path" {
			t.Errorf("%s parameter %q is in %q, want path", path, parameter.Name, parameter.Location)
		}
		if !parameter.Required {
			t.Errorf("%s path parameter %q is not required", path, parameter.Name)
		}
		actual = append(actual, parameter.Name)
	}
	sort.Strings(actual)
	sort.Strings(want)
	if fmt.Sprint(actual) != fmt.Sprint(want) {
		t.Errorf("OpenAPI path parameters for %s = %v, want %v", path, actual, want)
	}
}

func assertOperationContract(t *testing.T, document documentedAPI, path, method string, operation *documentedOperation, contract operationContract) {
	t.Helper()
	actualStatuses := make([]string, 0, len(operation.Responses))
	for status, response := range operation.Responses {
		actualStatuses = append(actualStatuses, status)
		if status != "204" {
			response = resolveResponse(document, response)
			_, jsonOK := response.Content["application/json"]
			binaryOK := false
			for _, mediaType := range []string{"image/jpeg", "image/png", "image/gif", "image/webp"} {
				if _, exists := response.Content[mediaType]; exists {
					binaryOK = true
				}
			}
			if !jsonOK && !(contract.binaryOK && status == "200" && binaryOK) {
				t.Errorf("%s %s response %s has no application/json content", method, path, status)
			}
		}
	}
	sort.Strings(actualStatuses)
	sort.Strings(contract.statuses)
	if fmt.Sprint(actualStatuses) != fmt.Sprint(contract.statuses) {
		t.Errorf("OpenAPI statuses for %s %s = %v, want %v", method, path, actualStatuses, contract.statuses)
	}
	if contract.requestJSON {
		if operation.RequestBody == nil {
			t.Errorf("%s %s is missing requestBody", method, path)
		} else if _, ok := operation.RequestBody.Content["application/json"]; !ok {
			t.Errorf("%s %s requestBody has no application/json content", method, path)
		}
	} else if contract.binaryRequest {
		if operation.RequestBody == nil {
			t.Errorf("%s %s is missing binary requestBody", method, path)
		} else if _, ok := operation.RequestBody.Content["image/png"]; !ok {
			t.Errorf("%s %s requestBody has no image/png content", method, path)
		}
	} else if operation.RequestBody != nil {
		t.Errorf("%s %s documents an unsupported request body", method, path)
	}
	actualQuery := make([]string, 0, len(operation.Parameters))
	for _, parameter := range operation.Parameters {
		parameter = resolveParameter(document, parameter)
		if parameter.Location == "query" {
			actualQuery = append(actualQuery, parameter.Name)
			switch parameter.Name {
			case "limit":
				if fmt.Sprint(parameter.Schema["type"]) != "integer" || fmt.Sprint(parameter.Schema["minimum"]) != "1" ||
					fmt.Sprint(parameter.Schema["maximum"]) != "200" || fmt.Sprint(parameter.Schema["default"]) != "50" {
					t.Errorf("%s %s limit schema = %#v", method, path, parameter.Schema)
				}
			case "cursor":
				if parameter.Schema["type"] != "string" || fmt.Sprint(parameter.Schema["maxLength"]) != "683" {
					t.Errorf("%s %s cursor schema = %#v", method, path, parameter.Schema)
				}
			}
		}
	}
	sort.Strings(actualQuery)
	sort.Strings(contract.query)
	if fmt.Sprint(actualQuery) != fmt.Sprint(contract.query) {
		t.Errorf("OpenAPI query parameters for %s %s = %v, want %v", method, path, actualQuery, contract.query)
	}
}

func operationFor(path documentedPath, method string) *documentedOperation {
	switch method {
	case http.MethodGet:
		return path.Get
	case http.MethodPost:
		return path.Post
	case http.MethodPatch:
		return path.Patch
	case http.MethodPut:
		return path.Put
	case http.MethodDelete:
		return path.Delete
	default:
		return nil
	}
}

func resolveParameter(document documentedAPI, parameter documentedParameter) documentedParameter {
	const prefix = "#/components/parameters/"
	if strings.HasPrefix(parameter.Ref, prefix) {
		return document.Components.Parameters[strings.TrimPrefix(parameter.Ref, prefix)]
	}
	return parameter
}

func resolveResponse(document documentedAPI, response documentedResponse) documentedResponse {
	const prefix = "#/components/responses/"
	if strings.HasPrefix(response.Ref, prefix) {
		return document.Components.Responses[strings.TrimPrefix(response.Ref, prefix)]
	}
	return response
}

func joinedMethods(methods map[string]operationContract) string {
	ordered := make([]string, 0, len(methods))
	for _, candidate := range []string{http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodPut, http.MethodDelete} {
		if _, ok := methods[candidate]; ok {
			ordered = append(ordered, candidate)
		}
	}
	return strings.Join(ordered, ", ")
}
