package media

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

var (
	ErrUnsafeURL      = errors.New("unsafe provider URL")
	ErrRedirectDenied = errors.New("provider redirects are disabled")
)

type Resolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type URLPolicyConfig struct {
	AllowedHosts          []string
	Resolver              Resolver
	DialContext           func(context.Context, string, string) (net.Conn, error)
	ConnectTimeout        time.Duration
	TLSHandshakeTimeout   time.Duration
	ResponseHeaderTimeout time.Duration
	TotalTimeout          time.Duration
}

type URLPolicy struct {
	hosts                 map[string]struct{}
	resolver              Resolver
	dialContext           func(context.Context, string, string) (net.Conn, error)
	connectTimeout        time.Duration
	tlsHandshakeTimeout   time.Duration
	responseHeaderTimeout time.Duration
	totalTimeout          time.Duration
}

type PinnedTarget struct {
	URL      *url.URL
	Hostname string
	Port     string
	IP       net.IP
}

func NewURLPolicy(config URLPolicyConfig) (*URLPolicy, error) {
	if len(config.AllowedHosts) == 0 {
		return nil, errors.New("provider URL hostname allowlist is required")
	}
	hosts := make(map[string]struct{}, len(config.AllowedHosts))
	for _, host := range config.AllowedHosts {
		host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
		if host == "" || net.ParseIP(host) != nil || strings.ContainsAny(host, "/:@") {
			return nil, ErrUnsafeURL
		}
		hosts[host] = struct{}{}
	}
	if config.Resolver == nil {
		config.Resolver = net.DefaultResolver
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 5 * time.Second
	}
	if config.TLSHandshakeTimeout == 0 {
		config.TLSHandshakeTimeout = 5 * time.Second
	}
	if config.ResponseHeaderTimeout == 0 {
		config.ResponseHeaderTimeout = 10 * time.Second
	}
	if config.TotalTimeout == 0 {
		config.TotalTimeout = 30 * time.Second
	}
	if config.ConnectTimeout < time.Millisecond || config.ConnectTimeout > 30*time.Second ||
		config.TLSHandshakeTimeout < time.Millisecond || config.TLSHandshakeTimeout > 30*time.Second ||
		config.ResponseHeaderTimeout < time.Millisecond || config.ResponseHeaderTimeout > time.Minute ||
		config.TotalTimeout < time.Millisecond || config.TotalTimeout > 5*time.Minute ||
		config.ConnectTimeout > config.TotalTimeout || config.TLSHandshakeTimeout > config.TotalTimeout || config.ResponseHeaderTimeout > config.TotalTimeout {
		return nil, ErrUnsafeURL
	}
	if config.DialContext == nil {
		config.DialContext = (&net.Dialer{Timeout: config.ConnectTimeout, KeepAlive: 30 * time.Second}).DialContext
	}
	return &URLPolicy{
		hosts: hosts, resolver: config.Resolver, dialContext: config.DialContext,
		connectTimeout: config.ConnectTimeout, tlsHandshakeTimeout: config.TLSHandshakeTimeout,
		responseHeaderTimeout: config.ResponseHeaderTimeout, totalTimeout: config.TotalTimeout,
	}, nil
}

func (policy *URLPolicy) ValidateAndPin(ctx context.Context, rawURL string) (PinnedTarget, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.Fragment != "" || parsed.Hostname() == "" {
		return PinnedTarget{}, ErrUnsafeURL
	}
	hostname := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	if _, allowed := policy.hosts[hostname]; !allowed || net.ParseIP(hostname) != nil {
		return PinnedTarget{}, ErrUnsafeURL
	}
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	if port != "443" {
		return PinnedTarget{}, ErrUnsafeURL
	}
	addresses, err := policy.resolver.LookupIPAddr(ctx, hostname)
	if err != nil || len(addresses) == 0 {
		return PinnedTarget{}, ErrUnsafeURL
	}
	var pinned net.IP
	for _, address := range addresses {
		if !publicProviderIP(address.IP) {
			return PinnedTarget{}, ErrUnsafeURL
		}
		if pinned == nil {
			pinned = append(net.IP(nil), address.IP...)
		}
	}
	return PinnedTarget{URL: parsed, Hostname: hostname, Port: port, IP: pinned}, nil
}

func (policy *URLPolicy) CheckRedirect(_ *http.Request, _ []*http.Request) error {
	return ErrRedirectDenied
}

func (policy *URLPolicy) Client(target PinnedTarget) *http.Client {
	transport := &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return policy.dialContext(ctx, network, net.JoinHostPort(target.IP.String(), target.Port))
		},
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12, ServerName: target.Hostname},
		TLSHandshakeTimeout:   policy.tlsHandshakeTimeout,
		ResponseHeaderTimeout: policy.responseHeaderTimeout,
		ExpectContinueTimeout: time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          8,
		MaxIdleConnsPerHost:   2,
	}
	return &http.Client{Transport: transport, CheckRedirect: policy.CheckRedirect, Timeout: policy.totalTimeout}
}

func publicProviderIP(ip net.IP) bool {
	address, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	address = address.Unmap()
	if !address.IsValid() || !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() || address.IsLinkLocalUnicast() || address.IsMulticast() || address.IsUnspecified() {
		return false
	}
	for _, prefix := range deniedProviderPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

var deniedProviderPrefixes = mustPrefixes(
	"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15",
	"198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
	"64:ff9b::/96", "64:ff9b:1::/48", "2001::/32", "2002::/16",
	"2001:db8::/32", "2001:10::/28", "2001:2::/48", "ff00::/8",
)

func mustPrefixes(values ...string) []netip.Prefix {
	output := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			panic(fmt.Sprintf("invalid static prefix %q", value))
		}
		output = append(output, prefix)
	}
	return output
}
