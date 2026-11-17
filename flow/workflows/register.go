package peerflow

import (
	"go.temporal.io/sdk/worker"
)

func RegisterFlowWorkerWorkflows(w worker.WorkflowRegistry) {
	// CDCFlowWorkflow: long-running workflow for a CDC mirror. Runs setup + initial snapshot (as children),
	// then the sync/normalize loop; handles pause/resume/terminate/resync signals and table additions,
	// and continues-as-new into itself or into DropFlowWorkflow.
	w.RegisterWorkflow(CDCFlowWorkflow)
	// DropFlowWorkflow: tears down a mirror (source slot/publication, destination tables, stats, catalog entries).
	// On resync it continues-as-new back into CDCFlowWorkflow.
	w.RegisterWorkflow(DropFlowWorkflow)
	// SetupFlowWorkflow: child of CDCFlowWorkflow; checks peer connections, sets up metadata tables,
	// ensures pullability, creates raw and normalized destination tables (optionally via pg_dump schema).
	w.RegisterWorkflow(SetupFlowWorkflow)
	// QRepFlowWorkflow: query-replication mirror; computes partitions from the watermark column,
	// replicates them via QRepPartitionWorkflow children, then waits for new rows and continues-as-new.
	w.RegisterWorkflow(QRepFlowWorkflow)
	// QRepWaitForNewRowsWorkflow: child of QRepFlowWorkflow; polls the source until rows newer than the
	// last partition appear (or the wait elapses in overwrite/full-refresh mode).
	w.RegisterWorkflow(QRepWaitForNewRowsWorkflow)
	// QRepPartitionWorkflow: child of QRepFlowWorkflow; replicates one batch of partitions to the destination.
	w.RegisterWorkflow(QRepPartitionWorkflow)
	// XminFlowWorkflow: QRep variant for Postgres sources using the xmin system column as watermark.
	w.RegisterWorkflow(XminFlowWorkflow)

	// GlobalScheduleManagerWorkflow: legacy manager that spawned cron children (HeartbeatFlowWorkflow,
	// RecordSlotSizeWorkflow). No longer started (the API kills existing runs at startup in favor of
	// ScheduledTasksWorkflow); kept registered so in-flight executions can still be processed.
	w.RegisterWorkflow(GlobalScheduleManagerWorkflow)
	// HeartbeatFlowWorkflow: legacy cron child of GlobalScheduleManagerWorkflow; sends Postgres WAL heartbeats.
	w.RegisterWorkflow(HeartbeatFlowWorkflow)
	// RecordSlotSizeWorkflow: legacy cron child of GlobalScheduleManagerWorkflow; records replication slot sizes.
	w.RegisterWorkflow(RecordSlotSizeWorkflow)
	// ScheduledTasksWorkflow: singleton started by the API at startup; runs the long-lived ScheduledTasks
	// activity (WAL heartbeats, slot size / metrics recording, flow config logging) on intervals.
	w.RegisterWorkflow(ScheduledTasksWorkflow)

	// StartMaintenanceWorkflow: before an upgrade, waits for running snapshots to finish, enables maintenance
	// mode, backs up the set of running mirrors and pauses them.
	w.RegisterWorkflow(StartMaintenanceWorkflow)
	// EndMaintenanceWorkflow: after an upgrade, resumes the mirrors backed up by StartMaintenanceWorkflow,
	// clears the backup and disables maintenance mode.
	w.RegisterWorkflow(EndMaintenanceWorkflow)

	// CancelTableAdditionFlow: cancels an in-progress table addition on a CDC mirror; keeps tables that already
	// finished snapshotting, updates the catalog, starts a fresh CDCFlowWorkflow and cleans up stats for the rest.
	w.RegisterWorkflow(CancelTableAdditionFlow)
}
