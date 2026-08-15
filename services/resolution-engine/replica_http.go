package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/openontology/resolution-engine/internal/crdt"
)

// The replication transport.
//
// State-based CRDTs make this almost trivial: there is no operation log to
// replay, no ordering requirement and no session. A peer's entire state is one
// GET, and folding it in is one function call. That is the whole reason the
// engine can survive an arbitrary partition — reconnecting is not a protocol,
// it is a fetch.

// replicaStatePath is the endpoint a peer pulls from.
const replicaStatePath = "/v1/replica/state"

// maxSnapshotBytes bounds what this replica will accept from a peer.
//
// A peer is not necessarily trusted just because it is reachable, and a
// state-based CRDT has no natural size limit — a compromised or misconfigured
// peer offering an unbounded body would otherwise be an out-of-memory kill on
// the ingestion path.
const maxSnapshotBytes = 64 << 20 // 64 MiB

// HTTPPeerClient fetches peer state over the replica endpoint.
type HTTPPeerClient struct {
	client *http.Client
}

// NewHTTPPeerClient builds a peer client.
//
// The per-request deadline comes from the caller's context (the sync loop
// applies REPLICA_SYNC_TIMEOUT), so the client itself carries no Timeout — one
// would silently override the budget the caller reasoned about.
func NewHTTPPeerClient() *HTTPPeerClient {
	return &HTTPPeerClient{
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost:   2,
				IdleConnTimeout:       90 * time.Second,
				ResponseHeaderTimeout: 30 * time.Second,
			},
		},
	}
}

// FetchState pulls one peer's replica snapshot.
func (c *HTTPPeerClient) FetchState(ctx context.Context, peer string) (crdt.Snapshot, error) {
	var snap crdt.Snapshot

	endpoint := strings.TrimSuffix(peer, "/") + replicaStatePath
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return snap, fmt.Errorf("build peer request for %s: %w", peer, err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return snap, fmt.Errorf("fetch peer state from %s: %w", peer, err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return snap, fmt.Errorf("peer %s returned %s", peer, resp.Status)
	}

	if err := json.NewDecoder(io.LimitReader(resp.Body, maxSnapshotBytes)).Decode(&snap); err != nil {
		return snap, fmt.Errorf("decode peer state from %s: %w", peer, err)
	}
	return snap, nil
}

// registerReplicaRoutes mounts the replication endpoints on the admin server.
func registerReplicaRoutes(mux *http.ServeMux, replica *TopologyReplica) {
	// GET /v1/replica/state — this replica's full state, for a peer to join.
	//
	// The whole state every time, not a delta. A state-based CRDT has no delta
	// to compute without knowing what the peer already has, and the digest is
	// what makes that cheap in practice: a peer that already matches can
	// compare hashes and skip the transfer.
	mux.HandleFunc(replicaStatePath, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"error": "replica state is read-only; use POST /v1/replica/merge to offer state",
			})
			return
		}
		if replica == nil || !replica.cfg.Enabled {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"error": "replication is disabled on this engine (set REPLICA_ENABLED=true)",
			})
			return
		}

		snapshot := replica.Snapshot()
		w.Header().Set("X-Replica-ID", replica.ID())
		w.Header().Set("X-Graph-Revision", replica.Digest())
		writeJSON(w, http.StatusOK, snapshot)
	})

	// POST /v1/replica/merge — offer state to this replica.
	//
	// The push counterpart of the pull loop, for a site that reconnects and
	// wants to hand its state over immediately rather than waiting to be asked.
	// The join is commutative and idempotent, so a peer that both pushes here
	// and is pulled from converges identically either way.
	mux.HandleFunc("/v1/replica/merge", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, map[string]any{
				"error": "use POST to offer replica state",
			})
			return
		}
		if replica == nil || !replica.cfg.Enabled {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"error": "replication is disabled on this engine (set REPLICA_ENABLED=true)",
			})
			return
		}

		var snapshot crdt.Snapshot
		if err := json.NewDecoder(io.LimitReader(r.Body, maxSnapshotBytes)).Decode(&snapshot); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": fmt.Sprintf("could not decode replica snapshot: %v", err),
			})
			return
		}

		if snapshot.ClientID == replica.ID() {
			// Merging a snapshot that claims this replica's own identity would
			// let a peer write into our timeline under our name, which breaks
			// the per-replica ownership the whole arbitration rests on.
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":      "snapshot claims this replica's identity",
				"replica_id": replica.ID(),
			})
			return
		}

		before := replica.Digest()
		if err := replica.MergeSnapshot(snapshot); err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
				"error": fmt.Sprintf("merge failed: %v", err),
			})
			return
		}
		after := replica.Digest()

		writeJSON(w, http.StatusOK, map[string]any{
			"merged":         true,
			"from_replica":   snapshot.ClientID,
			"vertices":       len(snapshot.Vertices),
			"edges":          len(snapshot.Edges),
			"changed":        before != after,
			"graph_revision": after,
			"lamport_clock":  replica.Clock(),
		})
	})
}
