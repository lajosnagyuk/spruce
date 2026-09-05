package broker

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"time"
)

// Recovery runs on the existing peer worker, only after a dropped copy. Each
// page reserves the existing queue budget before retaining cache pointers.
func (b *Broker) repairPeerStep(p *peer) bool {
	version := p.repairVersion.Load()
	if version == p.repairCompleted.Load() {
		if !b.checkPeerGap(p) {
			return false
		}
		version = p.repairVersion.Load()
	}
	if p.repairStarted == 0 {
		p.repairStarted = version
	}
	limit := min(b.cfg.ReplicationQueueBytes, max(int64(1<<20), b.cfg.MaxMessage+2*maxHeaders+1024))
	for {
		used := p.queuedBytes.Load()
		if used+limit > b.cfg.ReplicationQueueBytes {
			return false
		}
		if p.queuedBytes.CompareAndSwap(used, used+limit) {
			break
		}
	}
	defer func() { p.queuedBytes.Add(-limit); b.signalReplicationFreed() }()
	messages, next, valid := b.cache.page(p.repairCursor, limit)
	if !valid {
		p.repairCursor = ""
		b.metrics.RepairErrors.Add(1)
		return false
	}
	if len(messages) == 0 {
		p.repairCompleted.Store(p.repairStarted)
		p.repairStarted = 0
		p.repairCursor = ""
		return false
	}
	var body bytes.Buffer
	if err := writePeerBatch(&body, messages); err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, p.url+"/internal/repair", bytes.NewReader(body.Bytes()))
	req.Header.Set("Spruce-Peer-Token", b.cfg.PeerToken)
	req.Header.Set("Spruce-Cluster-ID", b.cfg.ClusterID)
	req.Header.Set("Spruce-Peer-Version", "2")
	req.Header.Set("Content-Type", "application/vnd.spruce.peer.v2")
	resp, err := b.client.Do(req)
	if err != nil {
		b.metrics.RepairErrors.Add(1)
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b.metrics.RepairErrors.Add(1)
		return false
	}
	b.metrics.RepairPages.Add(1)
	b.metrics.RepairMessages.Add(uint64(len(messages)))
	p.repairCursor = next
	return true
}

func (b *Broker) repairPendingPeers() int {
	n := 0
	for _, p := range b.peers {
		if p.repairVersion.Load() != p.repairCompleted.Load() || p.gapRepairVersion.Load() != p.gapRepairCompleted.Load() {
			n++
		}
	}
	return n
}

// A normal out-of-order RPC must not start a cache walk after its predecessor
// arrives. Confirm that the receiver still has a stalled gap before repairing.
func (b *Broker) checkPeerGap(p *peer) bool {
	version := p.gapRepairVersion.Load()
	if version == p.gapRepairCompleted.Load() || time.Now().UnixNano() < p.gapRepairAfter.Load() {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, p.url+"/internal/capabilities", nil)
	req.Header.Set("Spruce-Peer-Token", b.cfg.PeerToken)
	req.Header.Set("Spruce-Cluster-ID", b.cfg.ClusterID)
	resp, err := b.client.Do(req)
	if err != nil {
		b.metrics.RepairErrors.Add(1)
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b.metrics.RepairErrors.Add(1)
		return false
	}
	if resp.Header.Get("Spruce-Repair-Required") == "pending" {
		p.gapRepairAfter.Store(time.Now().Add(time.Second).UnixNano())
		return false
	}
	p.gapRepairCompleted.Store(version)
	p.gapRepairAfter.Store(0)
	if resp.Header.Get("Spruce-Repair-Required") == "false" {
		return false
	}
	p.repairVersion.Add(1)
	return true
}
