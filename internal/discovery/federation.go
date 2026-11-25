package discovery

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/integrated-recorder/core/internal/discoveryprotocol"
	"github.com/integrated-recorder/core/internal/globalidentity"
	"github.com/integrated-recorder/core/internal/identityprotocol"
)

const (
	maxFederatedProviders = 8
	maxProviderResponse   = 1 << 20
)

var (
	ErrFederationConfig  = errors.New("discovery federation configuration is invalid")
	ErrNoProvider        = errors.New("no discovery provider responded successfully")
	ErrPublishIncomplete = errors.New("advertisement did not reach two distinct configured providers")
	ErrHistoryIncomplete = errors.New("identity history did not reach every configured provider")
	ErrHistoryDisagrees  = errors.New("configured providers disagree on the current identity history")
)

type ProviderStatus struct {
	Host    string `json:"host"`
	Success bool   `json:"success"`
	Code    string `json:"code,omitempty"`
}

type FederatedLookup struct {
	Advertisements []discoveryprotocol.VerifiedAdvertisement
	ForkedNodeIDs  []string
	Providers      []ProviderStatus
	Complete       bool
}

// FederatedHistoryLookup is a comparison of signed public histories returned
// independently by every configured provider. Agreed is only an operational
// freshness signal under the configured-provider assumption; providers do
// not become identity signers or an authority capable of resolving forks.
type FederatedHistoryLookup struct {
	Bundles   []globalidentity.HistoryBundle
	Summary   globalidentity.HistorySummary
	Providers []ProviderStatus
	Complete  bool
	Agreed    bool
}

type FederationClient struct {
	providers []providerEndpoint
	client    *http.Client
}

type providerEndpoint struct {
	base *url.URL
	host string
}

func NewFederationClient(providerURLs []string, client *http.Client) (*FederationClient, error) {
	if len(providerURLs) < 2 || len(providerURLs) > maxFederatedProviders {
		return nil, ErrFederationConfig
	}
	providers := make([]providerEndpoint, 0, len(providerURLs))
	seenHosts := map[string]bool{}
	for _, raw := range providerURLs {
		parsed, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/" {
			return nil, ErrFederationConfig
		}
		host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
		if host == "" || seenHosts[host] {
			return nil, ErrFederationConfig
		}
		seenHosts[host] = true
		parsed.Path = ""
		providers = append(providers, providerEndpoint{base: parsed, host: host})
	}
	if client == nil {
		client = &http.Client{Timeout: 8 * time.Second}
	} else {
		copyClient := *client
		client = &copyClient
		if client.Timeout <= 0 || client.Timeout > 30*time.Second {
			client.Timeout = 8 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &FederationClient{providers: providers, client: client}, nil
}

// Publish posts one validated signed advertisement to every configured
// provider concurrently. Success requires at least two distinct configured
// providers to acknowledge it; this is availability policy, not identity
// authority or a claim that the providers are organizationally independent.
func (c *FederationClient) Publish(ctx context.Context, canonical, signature []byte, now time.Time) ([]ProviderStatus, error) {
	if c == nil || ctx == nil || now.IsZero() {
		return nil, ErrFederationConfig
	}
	if _, err := discoveryprotocol.Verify(canonical, signature, now); err != nil {
		return nil, discoveryprotocol.ErrInvalid
	}
	body, err := json.Marshal(signedValue{Canonical: base64.RawURLEncoding.EncodeToString(canonical), Signature: base64.RawURLEncoding.EncodeToString(signature)})
	if err != nil || len(body) > maxPublishBody {
		return nil, discoveryprotocol.ErrInvalid
	}
	statuses := c.callAll(func(provider providerEndpoint) ProviderStatus {
		endpoint := *provider.base
		endpoint.Path = "/v1/advertisements"
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(body))
		if err != nil {
			return ProviderStatus{Host: provider.host, Code: "request_invalid"}
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := c.client.Do(request)
		if err != nil {
			return ProviderStatus{Host: provider.host, Code: "transport_failed"}
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1025))
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || len(body) > 1024 {
			return ProviderStatus{Host: provider.host, Code: "response_invalid"}
		}
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
			return ProviderStatus{Host: provider.host, Code: "http_status"}
		}
		return ProviderStatus{Host: provider.host, Success: true}
	})
	successes := 0
	for _, status := range statuses {
		if status.Success {
			successes++
		}
	}
	if successes < 2 {
		return statuses, ErrPublishIncomplete
	}
	return statuses, nil
}

// PublishIdentityHistory mirrors a cryptographically valid public history to
// every configured provider. Partial success is reported as failure so a
// caller cannot mistake one stale mirror for the required federation.
func (c *FederationClient) PublishIdentityHistory(ctx context.Context, bundle globalidentity.HistoryBundle, now time.Time) ([]ProviderStatus, error) {
	if c == nil || ctx == nil || now.IsZero() {
		return nil, ErrFederationConfig
	}
	if _, err := globalidentity.SummarizeHistoryBundle(bundle); err != nil {
		return nil, globalidentity.ErrHistoryInvalid
	}
	body, err := json.Marshal(bundle)
	if err != nil || len(body) > maxHistoryBody {
		return nil, globalidentity.ErrHistoryInvalid
	}
	statuses := c.callAll(func(provider providerEndpoint) ProviderStatus {
		endpoint := *provider.base
		endpoint.Path = "/v1/identity-histories/" + url.PathEscape(bundle.IdentityID)
		request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint.String(), bytes.NewReader(body))
		if err != nil {
			return ProviderStatus{Host: provider.host, Code: "request_invalid"}
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := c.client.Do(request)
		if err != nil {
			return ProviderStatus{Host: provider.host, Code: "transport_failed"}
		}
		defer response.Body.Close()
		if response.ContentLength > 1024 {
			return ProviderStatus{Host: provider.host, Code: "response_invalid"}
		}
		if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusCreated {
			return ProviderStatus{Host: provider.host, Code: "http_status"}
		}
		return ProviderStatus{Host: provider.host, Success: true}
	})
	for _, status := range statuses {
		if !status.Success {
			return statuses, ErrHistoryIncomplete
		}
	}
	return statuses, nil
}

// LookupIdentityHistory requires complete, byte-equivalent histories from all
// configured HTTPS providers. It never picks a highest sequence from a
// disagreement. Every valid candidate is returned so the caller can persist
// fork evidence before denying authorization.
func (c *FederationClient) LookupIdentityHistory(ctx context.Context, identityID string, now time.Time) (FederatedHistoryLookup, error) {
	if c == nil || ctx == nil || now.IsZero() || identityID == "" {
		return FederatedHistoryLookup{}, ErrFederationConfig
	}
	type providerResponse struct {
		status ProviderStatus
		bundle globalidentity.HistoryBundle
		wire   []byte
	}
	responses := make(chan providerResponse, len(c.providers))
	for _, provider := range c.providers {
		provider := provider
		go func() {
			endpoint := *provider.base
			endpoint.Path = "/v1/identity-histories/" + url.PathEscape(identityID)
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
			if err != nil {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "request_invalid"}}
				return
			}
			request.Header.Set("Accept", "application/json")
			response, err := c.client.Do(request)
			if err != nil {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "transport_failed"}}
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "http_status"}}
				return
			}
			body, err := io.ReadAll(io.LimitReader(response.Body, maxHistoryBody+1))
			if err != nil || len(body) == 0 || len(body) > maxHistoryBody {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "response_too_large"}}
				return
			}
			if !strictJSONDocument(body) {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			var bundle globalidentity.HistoryBundle
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&bundle) != nil || decoder.Decode(&struct{}{}) != io.EOF || bundle.IdentityID != identityID {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			if _, err := globalidentity.SummarizeHistoryBundle(bundle); err != nil {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "history_invalid"}}
				return
			}
			canonical, err := json.Marshal(bundle)
			if err != nil {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			responses <- providerResponse{status: ProviderStatus{Host: provider.host, Success: true}, bundle: bundle, wire: canonical}
		}()
	}
	result := FederatedHistoryLookup{Bundles: []globalidentity.HistoryBundle{}, Providers: make([]ProviderStatus, 0, len(c.providers))}
	var agreedWire []byte
	for range c.providers {
		response := <-responses
		result.Providers = append(result.Providers, response.status)
		if response.status.Success {
			result.Bundles = append(result.Bundles, response.bundle)
			if agreedWire == nil {
				agreedWire = response.wire
				result.Summary, _ = globalidentity.SummarizeHistoryBundle(response.bundle)
			} else if !bytes.Equal(agreedWire, response.wire) {
				result.Agreed = false
			}
		}
	}
	sort.Slice(result.Providers, func(i, j int) bool { return result.Providers[i].Host < result.Providers[j].Host })
	result.Complete = len(result.Bundles) == len(c.providers)
	if result.Complete && agreedWire != nil {
		result.Agreed = true
		for _, bundle := range result.Bundles[1:] {
			wire, err := json.Marshal(bundle)
			if err != nil || !bytes.Equal(agreedWire, wire) {
				result.Agreed = false
				break
			}
		}
	}
	if !result.Complete {
		return result, ErrNoProvider
	}
	if !result.Agreed {
		return result, ErrHistoryDisagrees
	}
	return result, nil
}

// Lookup queries every configured provider and merges only Node-Key-verified,
// unexpired ads. Same NodeID/sequence with different signed bytes is excluded
// and reported as a fork. Different sequences are stale location updates, not
// identity-history successors; the highest advertisement sequence is selected
// only as a communication candidate and still requires independent identity
// history/delegation validation by the peer caller.
func (c *FederationClient) Lookup(ctx context.Context, identityHint string, now time.Time) (FederatedLookup, error) {
	if c == nil || ctx == nil || now.IsZero() {
		return FederatedLookup{}, ErrFederationConfig
	}
	if len(identityHint) != len("gih_")+22 || !strings.HasPrefix(identityHint, "gih_") {
		return FederatedLookup{}, ErrFederationConfig
	}
	type providerResponse struct {
		status ProviderStatus
		ads    []discoveryprotocol.VerifiedAdvertisement
	}
	responses := make(chan providerResponse, len(c.providers))
	for _, provider := range c.providers {
		provider := provider
		go func() {
			endpoint := *provider.base
			query := endpoint.Query()
			query.Set("identity_hint", identityHint)
			endpoint.Path = "/v1/advertisements"
			endpoint.RawQuery = query.Encode()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
			if err != nil {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "request_invalid"}}
				return
			}
			response, err := c.client.Do(request)
			if err != nil {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "transport_failed"}}
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "http_status"}}
				return
			}
			body, err := io.ReadAll(io.LimitReader(response.Body, maxProviderResponse+1))
			if err != nil || len(body) > maxProviderResponse {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "response_too_large"}}
				return
			}
			if _, err := identityprotocol.Canonicalize(body); err != nil {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			var payload struct {
				Advertisements []signedValue `json:"advertisements"`
			}
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF || len(payload.Advertisements) > maxLookupResults {
				responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			verifiedAds := make([]discoveryprotocol.VerifiedAdvertisement, 0, len(payload.Advertisements))
			for _, value := range payload.Advertisements {
				canonical, cErr := base64.RawURLEncoding.DecodeString(value.Canonical)
				signature, sErr := base64.RawURLEncoding.DecodeString(value.Signature)
				if cErr != nil || sErr != nil || base64.RawURLEncoding.EncodeToString(canonical) != value.Canonical || base64.RawURLEncoding.EncodeToString(signature) != value.Signature {
					responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "advertisement_invalid"}}
					return
				}
				verified, verifyErr := discoveryprotocol.Verify(canonical, signature, now)
				if verifyErr != nil || verified.Advertisement.GlobalIdentityHint != identityHint {
					responses <- providerResponse{status: ProviderStatus{Host: provider.host, Code: "advertisement_invalid"}}
					return
				}
				verifiedAds = append(verifiedAds, verified)
			}
			responses <- providerResponse{status: ProviderStatus{Host: provider.host, Success: true}, ads: verifiedAds}
		}()
	}
	result := FederatedLookup{Advertisements: []discoveryprotocol.VerifiedAdvertisement{}, ForkedNodeIDs: []string{}, Providers: make([]ProviderStatus, 0, len(c.providers))}
	successes := 0
	versions := map[string]map[string]discoveryprotocol.VerifiedAdvertisement{}
	forked := map[string]bool{}
	for range c.providers {
		response := <-responses
		result.Providers = append(result.Providers, response.status)
		if response.status.Success {
			successes++
		}
		for _, ad := range response.ads {
			nodeID, sequence := ad.Advertisement.NodeID, ad.Advertisement.Sequence
			if versions[nodeID] == nil {
				versions[nodeID] = map[string]discoveryprotocol.VerifiedAdvertisement{}
			}
			if previous, exists := versions[nodeID][sequence]; exists && (!bytes.Equal(previous.Canonical, ad.Canonical) || !bytes.Equal(previous.Signature, ad.Signature)) {
				forked[nodeID] = true
				continue
			}
			versions[nodeID][sequence] = ad
		}
	}
	if successes == 0 {
		return result, ErrNoProvider
	}
	result.Complete = successes == len(c.providers)
	for nodeID, records := range versions {
		if forked[nodeID] {
			result.ForkedNodeIDs = append(result.ForkedNodeIDs, nodeID)
			continue
		}
		var latest discoveryprotocol.VerifiedAdvertisement
		for _, candidate := range records {
			if latest.Advertisement.NodeID == "" || compareSequence(candidate.Advertisement.Sequence, latest.Advertisement.Sequence) > 0 {
				latest = candidate
			}
		}
		if latest.Advertisement.NodeID != "" {
			result.Advertisements = append(result.Advertisements, latest)
		}
	}
	sort.Strings(result.ForkedNodeIDs)
	sort.Slice(result.Advertisements, func(i, j int) bool {
		return result.Advertisements[i].Advertisement.NodeID < result.Advertisements[j].Advertisement.NodeID
	})
	sort.Slice(result.Providers, func(i, j int) bool { return result.Providers[i].Host < result.Providers[j].Host })
	return result, nil
}

func (c *FederationClient) callAll(call func(providerEndpoint) ProviderStatus) []ProviderStatus {
	statuses := make(chan ProviderStatus, len(c.providers))
	for _, provider := range c.providers {
		provider := provider
		go func() { statuses <- call(provider) }()
	}
	results := make([]ProviderStatus, 0, len(c.providers))
	for range c.providers {
		results = append(results, <-statuses)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Host < results[j].Host })
	return results
}

func (status ProviderStatus) String() string {
	if status.Success {
		return fmt.Sprintf("%s:ok", status.Host)
	}
	return fmt.Sprintf("%s:%s", status.Host, status.Code)
}
