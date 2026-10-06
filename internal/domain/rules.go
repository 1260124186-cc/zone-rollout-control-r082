package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"time"
)

func ActivatePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanDraft {
		return InvalidState("plan can be activated only from draft", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	if plan.LatestRevisionID == "" {
		return InvalidState("plan needs a zone revision before activation", map[string]any{
			"plan_id": plan.ID,
		})
	}
	plan.State = PlanActive
	plan.ActivatedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

func CompletePlan(plan *RolloutPlan, now time.Time) error {
	if plan.State != PlanActive {
		return InvalidState("plan can be completed only from active", map[string]any{
			"plan_id": plan.ID,
			"state":   plan.State,
		})
	}
	plan.State = PlanCompleted
	plan.CompletedAt = timePointer(now.UTC())
	plan.Version++
	return nil
}

type AllocationPlan struct {
	PlanID             string
	GroupID            string
	RequestKey         string
	RequestFingerprint string
	Region             string
	At                 time.Time
	ExpiresAt          time.Time
	Nodes              []ResolverNode
	LeaseIDs           []string
}

func NewAllocationPlan(
	planID string,
	groupID string,
	requestKey string,
	requestFingerprint string,
	region string,
	leaseSeconds int,
	nodes []ResolverNode,
	leaseIDs []string,
	now time.Time,
) (AllocationPlan, error) {
	if planID == "" || groupID == "" || len(nodes) == 0 {
		return AllocationPlan{}, NewError(CodeInternal, "allocation plan is incomplete", map[string]any{
			"plan_id": planID,
		})
	}
	if len(nodes) != len(leaseIDs) {
		return AllocationPlan{}, NewError(CodeInternal, "allocation plan node and lease counts differ", map[string]any{
			"nodes":  len(nodes),
			"leases": len(leaseIDs),
		})
	}
	nodeCopies := make([]ResolverNode, len(nodes))
	copy(nodeCopies, nodes)
	leaseCopies := make([]string, len(leaseIDs))
	copy(leaseCopies, leaseIDs)
	at := now.UTC()
	return AllocationPlan{
		PlanID:             planID,
		GroupID:            groupID,
		RequestKey:         requestKey,
		RequestFingerprint: requestFingerprint,
		Region:             region,
		At:                 at,
		ExpiresAt:          at.Add(time.Duration(leaseSeconds) * time.Second),
		Nodes:              nodeCopies,
		LeaseIDs:           leaseCopies,
	}, nil
}

// Commit materializes the prepared allocation as leases, node claims and a
// stored request index for the supplied state.
func (p AllocationPlan) Commit(state *State) (LeaseGroup, error) {
	requestIndex := p.PlanID + "\x00" + p.RequestKey
	if _, exists := state.LeaseRequestKeys[requestIndex]; exists {
		return LeaseGroup{}, NewError(CodeIdempotencyConflict, "lease request key was already committed", map[string]any{
			"request_key": p.RequestKey,
		})
	}
	leaseIDs := make([]string, 0, len(p.Nodes))
	for index, node := range p.Nodes {
		stored, ok := state.Nodes[node.ID]
		if !ok {
			return LeaseGroup{}, NewError(CodeStorageFailure, "allocation plan references a missing node", map[string]any{
				"node_id": node.ID,
			})
		}
		stored.State = NodeLeased
		stored.Version++
		stored.UpdatedAt = p.At
		state.Nodes[stored.ID] = stored
		leaseID := p.LeaseIDs[index]
		state.Leases[leaseID] = NodeLease{
			ID:           leaseID,
			PlanID:       p.PlanID,
			NodeID:       stored.ID,
			AllocationID: p.GroupID,
			State:        LeaseClaimed,
			ClaimedAt:    p.At,
			ExpiresAt:    p.ExpiresAt,
		}
		leaseIDs = append(leaseIDs, leaseID)
	}
	group := LeaseGroup{
		ID:                 p.GroupID,
		PlanID:             p.PlanID,
		RequestKey:         p.RequestKey,
		RequestFingerprint: p.RequestFingerprint,
		Region:             p.Region,
		LeaseIDs:           leaseIDs,
		CreatedAt:          p.At,
	}
	state.LeaseGroups[group.ID] = group
	state.LeaseRequestKeys[requestIndex] = group.ID
	return group, nil
}

func LeaseFingerprint(planID string, input LeaseInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%s|%d|%d",
		planID,
		input.RequestKey,
		input.Region,
		input.LeaseSeconds,
		input.MaxNodes,
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func ProbeFingerprint(leaseID string, input ProbeInput) string {
	payload := fmt.Sprintf(
		"%s|%s|%t|%d|%d|%s",
		leaseID,
		input.RequestKey,
		input.Success,
		input.ObservedSerial,
		input.LatencyMS,
		input.ObservedAt.UTC().Format(time.RFC3339Nano),
	)
	sum := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(sum[:])
}

func BuildAssessment(
	id string,
	plan RolloutPlan,
	revision ZoneRevision,
	probes []Probe,
	now time.Time,
) RolloutAssessment {
	reasons := make(map[string]struct{})
	if len(probes) == 0 {
		reasons["no_probes"] = struct{}{}
	}
	probeCount := len(probes)
	successCount := 0
	maximumLatency := 0
	nodes := make(map[string]struct{})
	for _, probe := range probes {
		nodes[probe.NodeID] = struct{}{}
		if probe.LatencyMS > maximumLatency {
			maximumLatency = probe.LatencyMS
		}
		if probe.Success {
			successCount++
			if probe.ObservedSerial != revision.Serial {
				reasons["serial_mismatch"] = struct{}{}
			}
		}
	}
	if len(nodes) < plan.ExpectedNodes {
		reasons["node_not_observed"] = struct{}{}
	}
	successRatio := 0.0
	if probeCount > 0 {
		successRatio = round(float64(successCount)/float64(probeCount), 6)
	}
	if successRatio < plan.MinimumSuccessRatio {
		reasons["success_ratio_below_minimum"] = struct{}{}
	}
	if maximumLatency > plan.MaximumLatencyMS {
		reasons["latency_limit_exceeded"] = struct{}{}
	}
	reasonList := make([]string, 0, len(reasons))
	for reason := range reasons {
		reasonList = append(reasonList, reason)
	}
	sort.Strings(reasonList)
	verdict := VerdictConsistent
	if len(reasonList) > 0 {
		verdict = VerdictReviewRequired
	}
	return RolloutAssessment{
		ID:               id,
		PlanID:           plan.ID,
		RevisionID:       revision.ID,
		Verdict:          verdict,
		Reasons:          reasonList,
		ProbeCount:       probeCount,
		SuccessCount:     successCount,
		SuccessRatio:     successRatio,
		MaximumLatencyMS: maximumLatency,
		CreatedAt:        now.UTC(),
	}
}

func round(value float64, places int) float64 {
	scale := math.Pow10(places)
	return math.Round(value*scale) / scale
}

func timePointer(value time.Time) *time.Time {
	return &value
}
