package media_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/internal/gateway/media"
)

type staticResolver map[string][]net.IPAddr

func (resolver staticResolver) LookupIPAddr(_ context.Context, host string) ([]net.IPAddr, error) {
	addresses, ok := resolver[host]
	if !ok {
		return nil, errors.New("not found")
	}
	return addresses, nil
}

func TestProviderURLPolicyRequiresAllowlistedPublicHTTPSAndPinsDNS(t *testing.T) {
	policy, err := media.NewURLPolicy(media.URLPolicyConfig{
		AllowedHosts: []string{"media.provider.example"},
		Resolver:     staticResolver{"media.provider.example": {{IP: net.ParseIP("8.8.8.8")}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := policy.ValidateAndPin(context.Background(), "https://media.provider.example/object")
	if err != nil {
		t.Fatalf("ValidateAndPin() error = %v", err)
	}
	if target.Hostname != "media.provider.example" || target.IP.String() != "8.8.8.8" || target.Port != "443" {
		t.Fatalf("pinned target = %+v", target)
	}
}

func TestProviderURLPolicyRejectsPrivateReservedRebindingAndURLTricks(t *testing.T) {
	resolver := staticResolver{
		"private.example":   {{IP: net.ParseIP("127.0.0.1")}},
		"linklocal.example": {{IP: net.ParseIP("169.254.169.254")}},
		"mixed.example":     {{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("10.0.0.7")}},
		"testnet.example":   {{IP: net.ParseIP("192.0.2.10")}},
	}
	policy, err := media.NewURLPolicy(media.URLPolicyConfig{
		AllowedHosts: []string{"private.example", "linklocal.example", "mixed.example", "testnet.example"}, Resolver: resolver,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, rawURL := range []string{
		"http://private.example/object",
		"https://user:pass@private.example/object",
		"https://unlisted.example/object",
		"https://private.example/object",
		"https://linklocal.example/object",
		"https://mixed.example/object",
		"https://testnet.example/object",
	} {
		t.Run(rawURL, func(t *testing.T) {
			if _, err := policy.ValidateAndPin(context.Background(), rawURL); !errors.Is(err, media.ErrUnsafeURL) {
				t.Fatalf("ValidateAndPin(%q) error = %v, want ErrUnsafeURL", rawURL, err)
			}
		})
	}
}

func TestProviderURLPolicyRejectsIPv6TransitionAndEmbeddedNonPublicIPv4(t *testing.T) {
	addresses := map[string]string{
		"nat64-private.example":    "64:ff9b:1::a00:1",
		"teredo.example":           "2001:0:4136:e378:8000:63bf:3fff:fdd2",
		"six-to-four.example":      "2002:0a00:0001::1",
		"well-known-nat64.example": "64:ff9b::c000:0201",
	}
	resolver := staticResolver{}
	hosts := make([]string, 0, len(addresses))
	for host, address := range addresses {
		hosts = append(hosts, host)
		resolver[host] = []net.IPAddr{{IP: net.ParseIP(address)}}
	}
	policy, err := media.NewURLPolicy(media.URLPolicyConfig{AllowedHosts: hosts, Resolver: resolver})
	if err != nil {
		t.Fatal(err)
	}
	for host := range addresses {
		if _, err = policy.ValidateAndPin(context.Background(), "https://"+host+"/object"); !errors.Is(err, media.ErrUnsafeURL) {
			t.Fatalf("transition address for %s accepted: %v", host, err)
		}
	}
}

func TestProviderURLPolicyRejectsEveryRedirect(t *testing.T) {
	policy, _ := media.NewURLPolicy(media.URLPolicyConfig{AllowedHosts: []string{"media.provider.example"}, Resolver: staticResolver{}})
	if err := policy.CheckRedirect(nil, nil); !errors.Is(err, media.ErrRedirectDenied) {
		t.Fatalf("CheckRedirect() error = %v", err)
	}
}

func TestProviderURLPolicyRejectsTimeoutsThatDisableOrUnboundNetworkIO(t *testing.T) {
	for name, mutate := range map[string]func(*media.URLPolicyConfig){
		"negative total":   func(config *media.URLPolicyConfig) { config.TotalTimeout = -time.Second },
		"excessive total":  func(config *media.URLPolicyConfig) { config.TotalTimeout = 6 * time.Minute },
		"negative connect": func(config *media.URLPolicyConfig) { config.ConnectTimeout = -time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			config := media.URLPolicyConfig{AllowedHosts: []string{"media.provider.example"}}
			mutate(&config)
			if _, err := media.NewURLPolicy(config); !errors.Is(err, media.ErrUnsafeURL) {
				t.Fatalf("NewURLPolicy() error = %v, want ErrUnsafeURL", err)
			}
		})
	}
}

func TestProviderURLPolicyDialsOnlyTheValidatedPinnedAddress(t *testing.T) {
	dialFailure := errors.New("stop before network")
	var dialed string
	policy, err := media.NewURLPolicy(media.URLPolicyConfig{
		AllowedHosts: []string{"media.provider.example"},
		Resolver:     staticResolver{"media.provider.example": {{IP: net.ParseIP("8.8.8.8")}}},
		DialContext: func(_ context.Context, _, address string) (net.Conn, error) {
			dialed = address
			return nil, dialFailure
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := policy.ValidateAndPin(context.Background(), "https://media.provider.example/object")
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := policy.Client(target).Transport.(*http.Transport)
	if !ok {
		t.Fatal("pinned client did not use an HTTP transport")
	}
	if _, err = transport.DialContext(context.Background(), "tcp", "media.provider.example:443"); !errors.Is(err, dialFailure) {
		t.Fatalf("DialContext error = %v", err)
	}
	if dialed != "8.8.8.8:443" {
		t.Fatalf("dialed address = %q, want pinned address", dialed)
	}
}
