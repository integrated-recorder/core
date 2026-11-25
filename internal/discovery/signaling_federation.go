package discovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/integrated-recorder/core/internal/signalingprotocol"
)

const maxSignalLookupBody = 8 << 20

var (
	ErrSignalPublishIncomplete = errors.New("peer signal did not reach two discovery providers")
	ErrSignalLookupUnavailable = errors.New("no discovery provider returned peer signals")
	ErrSignalProvidersDisagree = errors.New("discovery providers returned conflicting peer signals")
)

type FederatedSignalLookup struct {
	Messages  []signalingprotocol.Message
	Providers []ProviderStatus
	Complete  bool
}

type signalLookupPayload struct {
	Messages []signalingprotocol.Message `json:"messages"`
}

// PublishSignal sends a signed, recipient-bound envelope to each configured
// HTTPS provider. At least two distinct providers must acknowledge it. This
// is an availability policy only; recipients still verify identity freshness,
// Node Key delegation, signature, session binding, and durable replay state.
func (c *FederationClient) PublishSignal(ctx context.Context, message signalingprotocol.Message, now time.Time) ([]ProviderStatus, error) {
	if c == nil || ctx == nil || now.IsZero() {
		return nil, ErrFederationConfig
	}
	canonical, err := signalingprotocol.CanonicalMessage(message)
	if err != nil || len(canonical) > signalingprotocol.MaxEnvelopeBytes {
		return nil, signalingprotocol.ErrInvalid
	}
	issued, err := time.Parse(time.RFC3339, message.IssuedAt)
	if err != nil || now.Before(issued.Add(-signalingprotocol.MaxClockSkew)) || !now.Before(parseSignalExpiry(message)) {
		return nil, signalingprotocol.ErrStale
	}
	statuses := c.callAll(func(provider providerEndpoint) ProviderStatus {
		endpoint := *provider.base
		endpoint.Path = "/v1/signals"
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(canonical))
		if err != nil {
			return ProviderStatus{Host: provider.host, Code: "request_invalid"}
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := c.client.Do(request)
		if err != nil {
			return ProviderStatus{Host: provider.host, Code: "transport_failed"}
		}
		defer response.Body.Close()
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 1025))
		if readErr != nil || len(body) > 1024 {
			return ProviderStatus{Host: provider.host, Code: "response_invalid"}
		}
		if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
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
		return statuses, ErrSignalPublishIncomplete
	}
	return statuses, nil
}

// PublishPeerSignal adapts the federated relay operation to peertransport's
// small signaling broker interface.
func (c *FederationClient) PublishPeerSignal(ctx context.Context, message signalingprotocol.Message, now time.Time) error {
	_, err := c.PublishSignal(ctx, message, now)
	return err
}

// LookupSignals queries all configured providers and merges only structurally
// valid bounded envelopes. A conflicting byte representation for one message
// ID fails closed. The returned messages are untrusted until the addressed
// node verifies each signature against current identity history and consumes
// its durable replay key.
func (c *FederationClient) LookupSignals(ctx context.Context, targetNodeID string) (FederatedSignalLookup, error) {
	if c == nil || ctx == nil || !signalNodeIDRE.MatchString(targetNodeID) {
		return FederatedSignalLookup{}, ErrFederationConfig
	}
	type responseResult struct {
		status   ProviderStatus
		messages []signalingprotocol.Message
	}
	responses := make(chan responseResult, len(c.providers))
	for _, provider := range c.providers {
		provider := provider
		go func() {
			endpoint := *provider.base
			query := endpoint.Query()
			query.Set("target_node_id", targetNodeID)
			endpoint.Path = "/v1/signals"
			endpoint.RawQuery = query.Encode()
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
			if err != nil {
				responses <- responseResult{status: ProviderStatus{Host: provider.host, Code: "request_invalid"}}
				return
			}
			request.Header.Set("Accept", "application/json")
			response, err := c.client.Do(request)
			if err != nil {
				responses <- responseResult{status: ProviderStatus{Host: provider.host, Code: "transport_failed"}}
				return
			}
			defer response.Body.Close()
			if response.StatusCode != http.StatusOK {
				responses <- responseResult{status: ProviderStatus{Host: provider.host, Code: "http_status"}}
				return
			}
			body, err := io.ReadAll(io.LimitReader(response.Body, maxSignalLookupBody+1))
			if err != nil || len(body) == 0 || len(body) > maxSignalLookupBody || !strictJSONDocument(body) {
				responses <- responseResult{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			decoder := json.NewDecoder(bytes.NewReader(body))
			decoder.DisallowUnknownFields()
			var payload signalLookupPayload
			if decoder.Decode(&payload) != nil || decoder.Decode(&struct{}{}) != io.EOF || len(payload.Messages) > maxSignalMessagesPerNode {
				responses <- responseResult{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
				return
			}
			for _, message := range payload.Messages {
				if message.TargetNodeID != targetNodeID {
					responses <- responseResult{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
					return
				}
				if _, err := signalingprotocol.CanonicalMessage(message); err != nil {
					responses <- responseResult{status: ProviderStatus{Host: provider.host, Code: "response_invalid"}}
					return
				}
			}
			responses <- responseResult{status: ProviderStatus{Host: provider.host, Success: true}, messages: payload.Messages}
		}()
	}

	result := FederatedSignalLookup{Messages: []signalingprotocol.Message{}, Providers: make([]ProviderStatus, 0, len(c.providers))}
	byID := make(map[string]signalingprotocol.Message)
	canonicalByID := make(map[string][]byte)
	successes := 0
	for range c.providers {
		response := <-responses
		result.Providers = append(result.Providers, response.status)
		if response.status.Success {
			successes++
		}
		for _, message := range response.messages {
			canonical, err := signalingprotocol.CanonicalMessage(message)
			if err != nil {
				return FederatedSignalLookup{}, signalingprotocol.ErrInvalid
			}
			if previous, exists := canonicalByID[message.MessageID]; exists && !bytes.Equal(previous, canonical) {
				return FederatedSignalLookup{}, fmt.Errorf("%w: %s", ErrSignalProvidersDisagree, message.MessageID)
			}
			canonicalByID[message.MessageID] = canonical
			byID[message.MessageID] = message
		}
	}
	if successes == 0 {
		return result, ErrSignalLookupUnavailable
	}
	result.Complete = successes == len(c.providers)
	for _, message := range byID {
		result.Messages = append(result.Messages, message)
	}
	sort.Slice(result.Messages, func(i, j int) bool {
		if result.Messages[i].IssuedAt == result.Messages[j].IssuedAt {
			return result.Messages[i].MessageID < result.Messages[j].MessageID
		}
		return result.Messages[i].IssuedAt < result.Messages[j].IssuedAt
	})
	sort.Slice(result.Providers, func(i, j int) bool { return result.Providers[i].Host < result.Providers[j].Host })
	return result, nil
}

// LookupPeerSignals adapts the federated relay operation to peertransport's
// small signaling broker interface. Partial provider availability is exposed
// through the returned messages and does not elevate them to trusted input.
func (c *FederationClient) LookupPeerSignals(ctx context.Context, targetNodeID string) ([]signalingprotocol.Message, error) {
	lookup, err := c.LookupSignals(ctx, targetNodeID)
	return lookup.Messages, err
}

func parseSignalExpiry(message signalingprotocol.Message) time.Time {
	expires, _ := time.Parse(time.RFC3339, message.ExpiresAt)
	return expires
}
