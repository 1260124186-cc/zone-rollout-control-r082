package workflowprobe

import (
	"net/http"
	"sync"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runNodeLeaseAllocation() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-east-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-east-02", "east"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "lease.example", 1001, 2, 0.5, 100)
	if err != nil {
		return err
	}
	first, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(first.LeaseIDs) != 2 {
		return ensure(false, "first lease group must contain two leases")
	}
	replay, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if replay.ID != first.ID {
		return ensure(false, "lease request replay returned another group")
	}
	_, err = allocate(harness, plan.ID, "lease-request-0002", "east", 2)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+first.LeaseIDs[0]+"/release", nil, &released); err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease release did not update state")
	}
	return runLeaseCommitmentGuards()
}

// runLeaseCommitmentGuards exercises the commit-time invariants over the real
// HTTP surface: concurrent claims for a single node must produce exactly one
// valid lease, and a plan that is completed (or still draft) can never receive
// fresh leases even when a ready node is available.
func runLeaseCommitmentGuards() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "guard-node-01", "east"); err != nil {
		return err
	}
	active, _, err := createActivePlan(harness, "guard-active.example", 2001, 1, 1, 100)
	if err != nil {
		return err
	}

	// Concurrent distinct requests racing for the single ready node.
	const contenders = 8
	type claimResult struct {
		group domain.LeaseGroup
		err   error
	}
	results := make([]claimResult, contenders)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			g, claimErr := allocate(harness, active.ID, contenderKey(i), "east", 1)
			results[i] = claimResult{group: g, err: claimErr}
		}(i)
	}
	close(start)
	wg.Wait()

	winners := 0
	var winner domain.LeaseGroup
	for _, result := range results {
		if result.err == nil {
			winners++
			winner = result.group
			continue
		}
		if err := expectFailure(result.err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
			return err
		}
	}
	if winners != 1 || len(winner.LeaseIDs) != 1 {
		return ensure(false, "concurrent claims for one node must yield exactly one lease")
	}
	// Replaying the winning key returns the very same group.
	winnerReplay, err := allocate(harness, active.ID, winner.RequestKey, "east", 1)
	if err != nil {
		return err
	}
	if winnerReplay.ID != winner.ID {
		return ensure(false, "winning lease request replay returned another group")
	}

	// The claimed node must stay leased; a second node enrolled afterwards must
	// not let the loser's key turn into a late success (it is a fresh key that
	// still conflicts because the plan now has the winner occupying the region).
	leased, err := listNodes(harness, string(domain.NodeLeased), "east")
	if err != nil {
		return err
	}
	if len(leased.Items) != 1 {
		return ensure(false, "exactly one node must be leased after contention")
	}

	// Once the plan completes, an allocation with a fresh key must fail even
	// though an unrelated ready node exists: no new leases, no node/plan change.
	if _, err := enrollNode(harness, "guard-node-02", "east"); err != nil {
		return err
	}
	var completed domain.RolloutPlan
	if err := harness.call(http.MethodPatch, "/v1/plans/"+active.ID+"/complete",
		map[string]any{"expected_version": active.Version}, &completed); err != nil {
		return err
	}
	if completed.State != domain.PlanCompleted {
		return ensure(false, "plan did not complete")
	}
	_, err = allocate(harness, active.ID, "guard-after-complete", "east", 1)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	readyAfter, err := listNodes(harness, string(domain.NodeReady), "east")
	if err != nil {
		return err
	}
	if len(readyAfter.Items) != 1 {
		return ensure(false, "completed-plan allocation must not claim the ready node")
	}

	// A draft plan must never hand out leases either.
	draft, err := createDraftPlan(harness, "guard-draft.example")
	if err != nil {
		return err
	}
	_, err = allocate(harness, draft.ID, "guard-while-draft0", "east", 1)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	return nil
}

// contenderKey returns a distinct, validation-compliant request key per
// contender ("guard-contend-01" ... "guard-contend-08").
func contenderKey(i int) string {
	return "guard-contend-0" + string(rune('1'+i))
}

// createDraftPlan creates a plan that stays in draft (no revision, inactive).
func createDraftPlan(harness *harness, zone string) (domain.RolloutPlan, error) {
	var plan domain.RolloutPlan
	err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                zone,
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    100,
		ExpectedNodes:       1,
	}, &plan)
	return plan, err
}

// listNodes queries the paged node list filtered by state and region.
func listNodes(harness *harness, state string, region string) (domain.NodePage, error) {
	var page domain.NodePage
	path := "/v1/nodes?limit=100"
	if state != "" {
		path += "&state=" + state
	}
	if region != "" {
		path += "&region=" + region
	}
	if err := harness.call(http.MethodGet, path, nil, &page); err != nil {
		return domain.NodePage{}, err
	}
	return page, nil
}
