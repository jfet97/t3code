import { RunId, ThreadId, type WorktreeSetupSnapshot } from "@t3tools/contracts";
import { resolveVisibleWorktreeSetup } from "@t3tools/client-runtime/worktree-setup";
import { describe, expect, it, vi } from "vite-plus/test";
import {
  resolveWorktreeSetupSnapshot,
  stopStartingThread,
  stopThreadTarget,
} from "./worktree-setup-state";

const threadId = ThreadId.make("setup-thread");
const running: WorktreeSetupSnapshot = {
  threadId,
  sequence: 1,
  phase: "running",
  startedAt: "2026-09-17T00:00:00.000Z",
  endedAt: null,
  branch: "feature",
  baseRef: "main",
  worktreePath: null,
  setupScript: null,
  stages: [],
  error: null,
};

describe("mobile worktree setup handoff", () => {
  it("keeps the setup identity after its card retires so the working header can take over", () => {
    const done = { ...running, phase: "done" as const, sequence: 2 };
    const held = resolveWorktreeSetupSnapshot(threadId, done, running);
    expect(held).toBe(done);
    expect(
      resolveVisibleWorktreeSetup({
        live: held,
        recorded: null,
        turnStarted: false,
        followUpSent: false,
      }),
    ).toBe(done);
    expect(
      resolveVisibleWorktreeSetup({
        live: held,
        recorded: null,
        turnStarted: true,
        followUpSent: false,
      }),
    ).toBeNull();
    expect(resolveWorktreeSetupSnapshot(threadId, undefined, held)).toBe(done);
  });

  it.each(["failed", "cancelled"] as const)(
    "retains %s details after the stream closes",
    (phase) => {
      const settled = { ...running, phase, sequence: 3 };
      const held = resolveWorktreeSetupSnapshot(threadId, settled, running);
      expect(resolveWorktreeSetupSnapshot(threadId, null, held)).toBe(settled);
      expect(resolveWorktreeSetupSnapshot(threadId, running, held)).toBe(settled);
      expect(
        resolveVisibleWorktreeSetup({
          live: held,
          recorded: null,
          turnStarted: false,
          followUpSent: false,
        }),
      ).toBe(settled);
      expect(
        resolveVisibleWorktreeSetup({
          live: held,
          recorded: null,
          turnStarted: false,
          followUpSent: true,
        }),
      ).toBeNull();
    },
  );

  it("does not carry a previous thread's progress across navigation", () => {
    const otherId = ThreadId.make("other-thread");
    expect(resolveWorktreeSetupSnapshot(otherId, running, running)).toBeNull();
    const other = { ...running, threadId: otherId };
    expect(resolveWorktreeSetupSnapshot(otherId, other, running)).toBe(other);
  });
});

describe("mobile Stop while a thread starts", () => {
  it("interrupts a held or preparing run that has no cancellable worktree setup", () => {
    expect(stopThreadTarget(null)).toBe("run");
    expect(stopThreadTarget({ ...running, phase: "done" })).toBe("run");
    expect(
      stopThreadTarget({
        ...running,
        stages: [
          {
            id: "agent",
            status: "done",
            startedAt: null,
            endedAt: null,
            percent: null,
            detail: null,
            tail: [],
          },
        ],
      }),
    ).toBe("run");
  });

  it("cancels a worktree setup that is still running", () => {
    expect(stopThreadTarget(running)).toBe("worktree-setup");
  });
});

describe("stopStartingThread", () => {
  const knownRunId = RunId.make("known-run");

  it("asks the server for the current run when it can no longer cancel the setup", async () => {
    const interruptRun = vi.fn(async (_runId: RunId | undefined) => undefined);
    await stopStartingThread({
      worktreeSetup: running,
      knownRunId,
      cancelSetup: async () => false,
      interruptRun,
    });
    expect(interruptRun.mock.calls).toEqual([[undefined]]);
  });

  it("leaves the run alone once its running setup was cancelled", async () => {
    const interruptRun = vi.fn(async (_runId: RunId | undefined) => undefined);
    await stopStartingThread({
      worktreeSetup: running,
      knownRunId: null,
      cancelSetup: async () => true,
      interruptRun,
    });
    expect(interruptRun).not.toHaveBeenCalled();
  });

  it("interrupts a held run without asking to cancel a setup, by id when one is known", async () => {
    const cancelSetup = vi.fn(async () => true);
    const interruptRun = vi.fn(async (_runId: RunId | undefined) => undefined);
    await stopStartingThread({ worktreeSetup: null, knownRunId, cancelSetup, interruptRun });
    await stopStartingThread({ worktreeSetup: null, knownRunId: null, cancelSetup, interruptRun });
    expect(cancelSetup).not.toHaveBeenCalled();
    expect(interruptRun.mock.calls).toEqual([[knownRunId], [undefined]]);
  });
});
