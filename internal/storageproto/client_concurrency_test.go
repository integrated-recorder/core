package storageproto

import (
	"context"
	"net/http"
	"sync"
	"testing"
)

func TestClientRequestAuthorizationIsRaceSafeWithClose(t *testing.T) {
	client := newClient("/tmp/not-dialed.sock", []byte("test-private-token"))
	start := make(chan struct{})
	var workers sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for request := 0; request < 500; request++ {
				req, err := client.request(context.Background(), http.MethodGet, "/v1/descriptor", nil)
				if err != nil {
					t.Errorf("construct concurrent request: %v", err)
					return
				}
				if authorization := req.Header.Get("Authorization"); authorization != "Bearer test-private-token" && authorization != "Bearer " {
					t.Errorf("request observed partially cleared authorization token: %q", authorization)
					return
				}
			}
		}()
	}
	close(start)
	_ = client.Close()
	workers.Wait()
	for _, value := range client.token {
		if value != 0 {
			t.Fatal("Client.Close did not clear the private authorization token")
		}
	}
}
