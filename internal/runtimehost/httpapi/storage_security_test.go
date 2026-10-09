package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/integrated-recorder/core/internal/authn"
)

type storageSecurityController struct {
	fakeController
	storageCalls map[string]int
	configErr    error
}

const storageTestInstanceID = "si_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func (c *storageSecurityController) storageCall(name string) {
	if c.storageCalls == nil {
		c.storageCalls = make(map[string]int)
	}
	c.storageCalls[name]++
}

func (c *storageSecurityController) StorageStatus(context.Context) (StorageProviderStatus, error) {
	c.storageCall("status")
	return StorageProviderStatus{
		Primary:   PrimaryStorageStatus{Kind: "local", State: "ready"},
		Providers: []StorageProviderSummary{},
	}, nil
}

func (c *storageSecurityController) StorageConfig(context.Context, string) (StorageConfigView, error) {
	c.storageCall("config_read")
	return c.configView(), nil
}

func (c *storageSecurityController) ConfigureStorage(context.Context, string, StorageConfigRequest) (StorageConfigView, error) {
	c.storageCall("config_write")
	if c.configErr != nil {
		return StorageConfigView{}, c.configErr
	}
	return c.configView(), nil
}

func (c *storageSecurityController) ProbeStorage(context.Context, string) (StorageProbeResult, error) {
	c.storageCall("probe")
	return StorageProbeResult{State: "ready"}, nil
}

func (c *storageSecurityController) ActivateStorage(context.Context, string) (StorageProviderStatus, error) {
	c.storageCall("activate")
	return StorageProviderStatus{
		Primary:   PrimaryStorageStatus{Kind: "local", State: "ready"},
		Providers: []StorageProviderSummary{},
	}, nil
}

func (c *storageSecurityController) ListStorageInstances(context.Context) ([]StorageInstanceSummary, error) {
	c.storageCall("instance_list")
	return []StorageInstanceSummary{{ID: storageTestInstanceID, DisplayName: "Archive A", ProviderID: "fixture", ProviderName: "Fixture", DesiredSetID: strings.Repeat("a", 64), Health: "unknown"}}, nil
}

func (c *storageSecurityController) CreateStorageInstance(context.Context, StorageInstanceCreateRequest) (StorageInstanceSummary, error) {
	c.storageCall("instance_create")
	return StorageInstanceSummary{ID: storageTestInstanceID, DisplayName: "Archive A", ProviderID: "fixture", ProviderName: "Fixture", DesiredSetID: strings.Repeat("a", 64), Health: "unknown"}, nil
}

func (c *storageSecurityController) StorageInstanceConfig(context.Context, string) (StorageConfigView, error) {
	c.storageCall("instance_config_read")
	return c.configView(), nil
}

func (c *storageSecurityController) ConfigureStorageInstance(context.Context, string, StorageConfigRequest) (StorageConfigView, error) {
	c.storageCall("instance_config_write")
	return c.configView(), nil
}

func (c *storageSecurityController) ProbeStorageInstance(context.Context, string) (StorageProbeResult, error) {
	c.storageCall("instance_probe")
	return StorageProbeResult{State: "ready"}, nil
}

func (c *storageSecurityController) ActivateStorageInstance(context.Context, string) (StorageProviderStatus, error) {
	c.storageCall("instance_activate")
	return StorageProviderStatus{Primary: PrimaryStorageStatus{Kind: "local", State: "ready"}, Providers: []StorageProviderSummary{}}, nil
}

func (c *storageSecurityController) configView() StorageConfigView {
	return StorageConfigView{
		Values: map[string]json.RawMessage{
			"endpoint": json.RawMessage(`"https://storage.example.test"`),
		},
		ConfiguredSecrets: []string{"access_key_id", "secret_access_key"},
	}
}

func newStorageSecurityAPI(t *testing.T, controller *storageSecurityController) (http.Handler, authFixture) {
	t.Helper()
	fixture := testAuth(t)
	api, err := New(fixture.service, false, false, controller, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	return api, fixture
}

func storageSecurityRequest(api http.Handler, fixture authFixture, method, path, body string, authenticated, csrf bool) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if authenticated {
		request.AddCookie(&http.Cookie{Name: authn.SessionCookieName, Value: fixture.session.Token})
	}
	if csrf {
		request.Header.Set("X-CSRF-Token", fixture.session.CSRFToken)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response := httptest.NewRecorder()
	api.ServeHTTP(response, request)
	return response
}

func TestStorageGETRoutesRequireAuthenticationButNotCSRF(t *testing.T) {
	controller := &storageSecurityController{}
	api, fixture := newStorageSecurityAPI(t, controller)

	for _, tc := range []struct {
		path string
		call string
	}{
		{StorageProviderEndpoint, "status"},
		{StorageProvidersPrefix + "fixture/config", "config_read"},
		{StorageInstancesEndpoint, "instance_list"},
		{StorageInstancesPrefix + storageTestInstanceID + "/config", "instance_config_read"},
	} {
		if response := storageSecurityRequest(api, fixture, http.MethodGet, tc.path, "", false, false); response.Code != http.StatusUnauthorized {
			t.Errorf("anonymous GET %s status=%d, want %d", tc.path, response.Code, http.StatusUnauthorized)
		}
		if controller.storageCalls[tc.call] != 0 {
			t.Errorf("anonymous GET %s reached controller", tc.path)
		}

		response := storageSecurityRequest(api, fixture, http.MethodGet, tc.path, "", true, false)
		if response.Code != http.StatusOK {
			t.Errorf("authenticated GET %s without CSRF status=%d body=%s", tc.path, response.Code, response.Body.String())
		}
		if controller.storageCalls[tc.call] != 1 {
			t.Errorf("authenticated GET %s controller calls=%d, want 1", tc.path, controller.storageCalls[tc.call])
		}
	}
}

func TestStorageMutationsRequireAuthenticationAndCSRF(t *testing.T) {
	controller := &storageSecurityController{}
	api, fixture := newStorageSecurityAPI(t, controller)
	configBody := `{"values":{"endpoint":"https://storage.example.test"},"secrets":{"access_key_id":"test-access","secret_access_key":"test-secret"}}`
	for _, tc := range []struct {
		method string
		path   string
		body   string
		call   string
	}{
		{http.MethodPut, StorageProvidersPrefix + "fixture/config", configBody, "config_write"},
		{http.MethodPost, StorageProvidersPrefix + "fixture/probe", `{}`, "probe"},
		{http.MethodPost, StorageProvidersPrefix + "fixture/activate", `{}`, "activate"},
		{http.MethodPost, StorageInstancesEndpoint, `{"display_name":"Archive A","provider_id":"fixture","values":{"endpoint":"https://storage.example.test"},"secrets":{"access_key_id":"a","secret_access_key":"b"}}`, "instance_create"},
		{http.MethodPut, StorageInstancesPrefix + storageTestInstanceID + "/config", configBody, "instance_config_write"},
		{http.MethodPost, StorageInstancesPrefix + storageTestInstanceID + "/probe", `{}`, "instance_probe"},
		{http.MethodPost, StorageInstancesPrefix + storageTestInstanceID + "/activate", `{}`, "instance_activate"},
	} {
		if response := storageSecurityRequest(api, fixture, tc.method, tc.path, tc.body, false, false); response.Code != http.StatusUnauthorized {
			t.Errorf("anonymous %s %s status=%d, want %d", tc.method, tc.path, response.Code, http.StatusUnauthorized)
		}
		if controller.storageCalls[tc.call] != 0 {
			t.Errorf("anonymous %s %s reached controller", tc.method, tc.path)
		}

		if response := storageSecurityRequest(api, fixture, tc.method, tc.path, tc.body, true, false); response.Code != http.StatusForbidden {
			t.Errorf("authenticated %s %s without CSRF status=%d, want %d", tc.method, tc.path, response.Code, http.StatusForbidden)
		}
		if controller.storageCalls[tc.call] != 0 {
			t.Errorf("CSRF-rejected %s %s reached controller", tc.method, tc.path)
		}

		response := storageSecurityRequest(api, fixture, tc.method, tc.path, tc.body, true, true)
		if response.Code != http.StatusOK {
			t.Errorf("authenticated %s %s with CSRF status=%d body=%s", tc.method, tc.path, response.Code, response.Body.String())
		}
		if controller.storageCalls[tc.call] != 1 {
			t.Errorf("authorized %s %s controller calls=%d, want 1", tc.method, tc.path, controller.storageCalls[tc.call])
		}
	}
}

func TestStorageConfigResponseAndErrorsDoNotExposeSecretsOrEndpointDetails(t *testing.T) {
	const (
		secretValue = "secret-value-must-never-appear"
		endpoint    = "https://private.example.test/upload?credential=query-value-must-not-appear"
	)
	controller := &storageSecurityController{}
	api, fixture := newStorageSecurityAPI(t, controller)
	body := `{"values":{"endpoint":"` + endpoint + `"},"secrets":{"secret_access_key":"` + secretValue + `"}}`
	response := storageSecurityRequest(api, fixture, http.MethodPut, StorageProvidersPrefix+"fixture/config", body, true, true)
	if response.Code != http.StatusOK {
		t.Fatalf("configure status=%d body=%s", response.Code, response.Body.String())
	}
	responseBody := response.Body.String()
	if strings.Contains(responseBody, secretValue) || strings.Contains(responseBody, "query-value-must-not-appear") {
		t.Fatalf("storage config response exposed sensitive material: %s", responseBody)
	}
	if !strings.Contains(responseBody, `"endpoint":"https://storage.example.test"`) || !strings.Contains(responseBody, `"secret_access_key"`) {
		t.Fatalf("storage config response omitted non-secret value or configured secret key: %s", responseBody)
	}

	controller.configErr = errors.New("provider failed with " + secretValue + " at " + endpoint)
	errorResponse := storageSecurityRequest(api, fixture, http.MethodPut, StorageProvidersPrefix+"fixture/config?token=query-value-must-not-appear", body, true, true)
	if errorResponse.Code != http.StatusInternalServerError {
		t.Fatalf("configure error status=%d body=%s", errorResponse.Code, errorResponse.Body.String())
	}
	errorBody := errorResponse.Body.String()
	for _, forbidden := range []string{secretValue, "private.example.test", "query-value-must-not-appear", "credential="} {
		if strings.Contains(errorBody, forbidden) {
			t.Fatalf("storage API error exposed %q: %s", forbidden, errorBody)
		}
	}
}
