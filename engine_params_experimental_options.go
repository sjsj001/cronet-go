package cronet

import (
	"encoding/json"
	"strings"

	E "github.com/sagernet/sing/common/exceptions"
)

func (p EngineParams) SetExperimentalOption(key string, value any) error {
	options := strings.TrimSpace(p.ExperimentalOptions())

	experimentalOptions := make(map[string]any)
	if options != "" {
		if err := json.Unmarshal([]byte(options), &experimentalOptions); err != nil {
			return err
		}
	}

	if value == nil {
		delete(experimentalOptions, key)
	} else {
		experimentalOptions[key] = value
	}

	encoded, err := json.Marshal(experimentalOptions)
	if err != nil {
		return err
	}
	p.SetExperimentalOptions(string(encoded))
	return nil
}

func (p EngineParams) SetAsyncDNS(enable bool) error {
	if !enable {
		return p.SetExperimentalOption("AsyncDNS", nil)
	}
	return p.SetExperimentalOption("AsyncDNS", map[string]any{
		"enable": true,
	})
}

// SetDNSServerOverride configures Cronet's built-in DNS client to exclusively use the
// provided nameserver addresses.
//
// The nameserver entries must be IP literals, in "ip:port" form (IPv6 in "[ip]:port"
// form). Passing an empty slice disables the override.
func (p EngineParams) SetDNSServerOverride(nameservers []string) error {
	if len(nameservers) == 0 {
		return p.SetExperimentalOption("DnsServerOverride", nil)
	}
	return p.SetExperimentalOption("DnsServerOverride", map[string]any{
		"nameservers": nameservers,
	})
}

// SetHostResolverRules sets rules to override DNS resolution.
// Format: "MAP hostname ip" or "MAP *.example.com ip" or "EXCLUDE hostname".
// Multiple rules can be separated by commas: "MAP foo 1.2.3.4, MAP bar 5.6.7.8".
// See net/dns/mapped_host_resolver.h for full format.
func (p EngineParams) SetHostResolverRules(rules string) error {
	if rules == "" {
		return p.SetExperimentalOption("HostResolverRules", nil)
	}
	return p.SetExperimentalOption("HostResolverRules", map[string]any{
		"host_resolver_rules": rules,
	})
}

// SetUseDnsHttpsSvcb enables or disables DNS HTTPS SVCB record lookups.
// When enabled, Chromium will query DNS for HTTPS records (type 65) which can
// contain ECH (Encrypted Client Hello) configurations and ALPN hints.
// This is required for ECH support.
func (p EngineParams) SetUseDnsHttpsSvcb(enable bool) error {
	return p.SetExperimentalOption("UseDnsHttpsSvcb", map[string]any{
		"enable": enable,
	})
}

// SetHTTP2Options configures the HTTP/2 receive windows. initialWindowSize is
// the stream window; with streamWindowAutoTuning it is the most a stream's
// window may grow to, and a session starts its streams at what 1200Mbit/s
// needs over the path's RTT (8MB at least) when the transport reports one.
// A libcronet built without patches/naiveproxy/0002 ignores the switch.
// maxFrameSize, when not zero, is announced as SETTINGS_MAX_FRAME_SIZE and is
// the largest frame the session then accepts (16384 to 2^24-1; ignored
// without patches/naiveproxy/0003).
func (p EngineParams) SetHTTP2Options(sessionMaxReceiveWindowSize, initialWindowSize uint64, streamWindowAutoTuning bool, maxFrameSize uint32) error {
	if maxFrameSize != 0 && (maxFrameSize < 1<<14 || maxFrameSize > 1<<24-1) {
		return E.New("HTTP/2 max frame size ", maxFrameSize, " is outside 16384 to 16777215")
	}
	options := map[string]any{
		"session_max_recv_window_size": sessionMaxReceiveWindowSize,
		"initial_window_size":          initialWindowSize,
		"stream_window_auto_tuning":    streamWindowAutoTuning,
	}
	if maxFrameSize != 0 {
		options["max_frame_size"] = maxFrameSize
	}
	return p.SetExperimentalOption("HTTP2Options", options)
}

// SetQUICOptions configures Cronet's QUIC parameters.
//
// connectionOptions is advertised to the server as the QUIC COPT tag list and only
// selects behavior on the peer; client_connection_options is never serialized and is
// the only tag list QUICHE consults for local client behavior, including the send-side
// congestion control algorithm (QuicConfig::HasClientRequestedIndependentOption reads
// client_connection_options_ when the perspective is client).
func (p EngineParams) SetQUICOptions(connectionOptions string, clientConnectionOptions string, initialStreamRecvWindowSize, initialSessionRecvWindowSize uint64) error {
	options := map[string]any{}
	if connectionOptions != "" {
		options["connection_options"] = connectionOptions
	}
	if clientConnectionOptions != "" {
		options["client_connection_options"] = clientConnectionOptions
	}
	if initialStreamRecvWindowSize > 0 {
		options["initial_stream_recv_window_size"] = initialStreamRecvWindowSize
	}
	if initialSessionRecvWindowSize > 0 {
		options["initial_session_recv_window_size"] = initialSessionRecvWindowSize
	}
	if len(options) == 0 {
		return p.SetExperimentalOption("QUIC", nil)
	}
	return p.SetExperimentalOption("QUIC", options)
}

func (p EngineParams) SetSocketPoolOptions(maxPerPool, maxPerProxyChain, maxPerGroup int) error {
	return p.SetExperimentalOption("SocketPoolOptions", map[string]any{
		"max_sockets_per_pool":        maxPerPool,
		"max_sockets_per_proxy_chain": maxPerProxyChain,
		"max_sockets_per_group":       maxPerGroup,
	})
}
