import type { RunId, ThreadId, WorktreeSetupSnapshot } from "@t3tools/contracts";
import { worktreeSetupAgentStarted } from "@t3tools/client-runtime/worktree-setup";

/** A subscription can close or replay an older value during the setup handoff. */
export function resolveWorktreeSetupSnapshot(
  threadId: ThreadId | null,
  latest: WorktreeSetupSnapshot | null | undefined,
  held: WorktreeSetupSnapshot | null,
): WorktreeSetupSnapshot | null {
  const current = latest?.threadId === threadId ? latest : null;
  const previous = held?.threadId === threadId ? held : null;
  return current && (!previous || current.sequence >= previous.sequence) ? current : previous;
}

/**
 * What Stop ends while a new thread starts: a worktree setup the server can still cancel, or
 * otherwise the run itself, such as a first message held for pre-start monitors.
 */
export function stopThreadTarget(
  worktreeSetup: WorktreeSetupSnapshot | null,
): "worktree-setup" | "run" {
  return worktreeSetup !== null &&
    worktreeSetup.phase === "running" &&
    !worktreeSetupAgentStarted(worktreeSetup)
    ? "worktree-setup"
    : "run";
}

/**
 * Stops a thread that is still starting. A running setup is cancelled first; when the server
 * reports nothing to cancel, because the handoff to the run began or the snapshot was stale,
 * the run itself is interrupted so Stop never does nothing. Without a known run, the interrupt
 * names none and the server stops the run its current projection shows.
 */
export async function stopStartingThread(input: {
  readonly worktreeSetup: WorktreeSetupSnapshot | null;
  readonly knownRunId: RunId | null;
  readonly cancelSetup: () => Promise<boolean>;
  readonly interruptRun: (runId: RunId | undefined) => Promise<unknown>;
}): Promise<void> {
  if (stopThreadTarget(input.worktreeSetup) === "worktree-setup") {
    if (await input.cancelSetup()) return;
    // the run began after this render, so the run it knew of may not be the one to stop
    await input.interruptRun(undefined);
    return;
  }
  await input.interruptRun(input.knownRunId ?? undefined);
}
