package allocation

import (
	"context"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/fleet"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

func (s *Service) Allocate(
	ctx context.Context,
	planID string,
	input domain.LeaseInput,
) (domain.LeaseGroup, error) {
	if err := domain.ValidateLeaseInput(input); err != nil {
		return domain.LeaseGroup{}, err
	}
	input.Region = strings.TrimSpace(strings.ToLower(input.Region))
	requestIndex := planID + "\x00" + input.RequestKey
	fingerprint := domain.LeaseFingerprint(planID, input)
	var group domain.LeaseGroup

	// Fast path for idempotent replays. A committed lease group is immutable, so
	// it is safe to serve from a read-only transaction; this keeps replays from
	// touching the snapshot (or bumping its revision) and ensures a duplicate
	// request always returns the first result, regardless of later plan state.
	err := s.store.Read(ctx, func(state *domain.State) error {
		if existingID, exists := state.LeaseRequestKeys[requestIndex]; exists {
			resolved, ok := state.LeaseGroups[existingID]
			if !ok {
				return domain.NewError(domain.CodeStorageFailure, "lease request index is inconsistent", map[string]any{
					"lease_group_id": existingID,
				})
			}
			if resolved.RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different lease parameters", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			group = resolved
			return nil
		}
		return nil
	})
	if err != nil {
		return domain.LeaseGroup{}, err
	}
	if group.ID != "" {
		return group, nil
	}

	// Expiry cleanup, candidate selection, plan-state validation and the commit
	// all happen inside one write transaction. A lease therefore counts only if
	// the state at the actual commit instant still permits it: a node already
	// taken by a concurrent allocation, or a plan completed in the meantime, is
	// visible here instead of being checked against a stale earlier snapshot.
	err = s.store.Write(ctx, func(state *domain.State) error {
		now := s.clock.Now().UTC()
		expireLeases(state, now)

		// Close the read/write gap for a request key that committed on another
		// goroutine after the read-only fast path above.
		if existingID, exists := state.LeaseRequestKeys[requestIndex]; exists {
			resolved, ok := state.LeaseGroups[existingID]
			if !ok {
				return domain.NewError(domain.CodeStorageFailure, "lease request index is inconsistent", map[string]any{
					"lease_group_id": existingID,
				})
			}
			if resolved.RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different lease parameters", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			group = resolved
			return nil
		}

		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active to claim leases", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}

		candidates := fleet.SelectForLease(state, input.Region, input.MaxNodes)
		if len(candidates) == 0 {
			return domain.NewError(domain.CodeNodeUnavailable, "no eligible node is ready", map[string]any{
				"plan_id": planID,
				"region":  input.Region,
			})
		}

		groupID := s.ids.New("grp")
		leaseIDs := make([]string, 0, len(candidates))
		for range candidates {
			leaseIDs = append(leaseIDs, s.ids.New("lse"))
		}
		allocation, err := domain.NewAllocationPlan(
			planID,
			groupID,
			input.RequestKey,
			fingerprint,
			input.Region,
			input.LeaseSeconds,
			candidates,
			leaseIDs,
			now,
		)
		if err != nil {
			return err
		}
		committed, err := allocation.Commit(state)
		if err != nil {
			return err
		}
		group = committed
		store.AddAudit(state, s.ids, "lease_group", group.ID, "created", now, map[string]any{
			"plan_id":     planID,
			"lease_count": len(group.LeaseIDs),
		})
		return nil
	})
	return group, err
}

func (s *Service) Release(ctx context.Context, leaseID string) (domain.NodeLease, error) {
	now := s.clock.Now().UTC()
	var result domain.NodeLease
	err := s.store.Write(ctx, func(state *domain.State) error {
		lease, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		if lease.State == domain.LeaseReleased || lease.State == domain.LeaseExpired {
			result = lease
			return nil
		}
		if lease.State != domain.LeaseClaimed {
			return domain.InvalidState("only a claimed lease can be released", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		finishedAt := now
		lease.State = domain.LeaseReleased
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		store.AddAudit(state, s.ids, "lease", leaseID, "released", now, nil)
		result = lease
		return nil
	})
	return result, err
}

func (s *Service) ExpireDue(ctx context.Context) (int, error) {
	now := s.clock.Now().UTC()
	expired := 0
	err := s.store.Write(ctx, func(state *domain.State) error {
		expired = expireLeases(state, now)
		return nil
	})
	return expired, err
}

func expireLeases(state *domain.State, now time.Time) int {
	count := 0
	for leaseID, lease := range state.Leases {
		if lease.State != domain.LeaseClaimed || lease.ExpiresAt.After(now) {
			continue
		}
		finishedAt := now
		lease.State = domain.LeaseExpired
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node, ok := state.Nodes[lease.NodeID]; ok && node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		count++
	}
	return count
}
