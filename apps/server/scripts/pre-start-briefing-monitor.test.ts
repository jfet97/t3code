import {
  CommandId,
  MessageId,
  ORCHESTRATION_V2_WS_METHODS,
  ProjectId,
  RunId,
  ThreadId,
  type OrchestrationV2RespondPreStartInput,
} from "@t3tools/contracts";
import { assert, describe, expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import {
  matchProject,
  monitorProject,
  type PreStartMonitorClient,
} from "./pre-start-briefing-monitor.ts";

const project = (id: string, workspaceRoot: string, title: string) => ({
  id: ProjectId.make(id),
  workspaceRoot,
  title,
});

describe("matchProject", () => {
  const projects = [
    project("project-api", "/work/api", "Service"),
    project("project-web", "/work/web", "Service"),
    project("project-docs", "/work/docs", "Docs"),
  ];

  it("finds a project by id, folder or unique title", () => {
    expect(matchProject(projects, "project-web")).toEqual({ id: "project-web" });
    expect(matchProject(projects, "/work/api")).toEqual({ id: "project-api" });
    expect(matchProject(projects, "Docs")).toEqual({ id: "project-docs" });
  });

  it("refuses a title several projects share instead of picking one", () => {
    expect(matchProject(projects, "Service")).toEqual({
      problem: "Several projects match Service; pass its id.",
    });
    expect(matchProject(projects, "Missing")).toEqual({ problem: "No project matches Missing." });
  });
});

describe("monitorProject", () => {
  const projectId = ProjectId.make("project-api");
  const heldThreadId = ThreadId.make("thread-new");
  const thread = (id: string, fields: Record<string, unknown>) => ({
    id: ThreadId.make(id),
    projectId,
    title: id,
    branch: null,
    archivedAt: null,
    activityRunStatus: "running",
    activeRunId: RunId.make(`${id}:run`),
    ...fields,
  });

  /** A connection that holds one first message, due ten seconds from now. */
  const fakeConnection = (options: {
    readonly shell: Effect.Effect<ReadonlyArray<ReturnType<typeof thread>>, string>;
    readonly projection?: (threadId: ThreadId) => Effect.Effect<unknown, string>;
  }) =>
    Effect.gen(function* () {
      const answers = yield* Queue.unbounded<OrchestrationV2RespondPreStartInput>();
      const client = {
        [ORCHESTRATION_V2_WS_METHODS.subscribePreStart]: () =>
          Stream.make(
            { kind: "ready", subscriptionId: "subscription" },
            {
              kind: "held",
              projectId,
              threadId: heldThreadId,
              runId: RunId.make("run-new"),
              messageId: MessageId.make("message-new"),
              requestId: CommandId.make("request-new"),
              expiresAt: "1970-01-01T00:00:10.000Z",
              text: "Fix the login bug",
            },
          ).pipe(Stream.concat(Stream.never)),
        [ORCHESTRATION_V2_WS_METHODS.respondPreStart]: (
          input: OrchestrationV2RespondPreStartInput,
        ) => Queue.offer(answers, input).pipe(Effect.as({ accepted: true })),
        [ORCHESTRATION_V2_WS_METHODS.subscribeShell]: () =>
          Stream.fromEffect(
            options.shell.pipe(
              Effect.map((threads) => ({ kind: "snapshot", snapshot: { projects: [], threads } })),
            ),
          ),
        [ORCHESTRATION_V2_WS_METHODS.getThreadProjection]: (input: { threadId: ThreadId }) =>
          options.projection?.(input.threadId) ?? Effect.fail("unreadable"),
      } as unknown as PreStartMonitorClient;
      yield* monitorProject(client, { projectId }).pipe(Effect.forkScoped);
      return answers;
    });

  it.effect("briefs each active thread's current request, not the messages queued after it", () =>
    Effect.gen(function* () {
      const answers = yield* fakeConnection({
        shell: Effect.succeed([
          thread("billing", { title: "Billing", branch: "billing-tables" }),
          thread("docs", { title: "Docs" }),
          thread(heldThreadId, {}),
          thread("idle", { activityRunStatus: null }),
          thread("archived", { archivedAt: "1970-01-01T00:00:00.000Z" }),
          thread("elsewhere", { projectId: ProjectId.make("project-web") }),
        ]),
        projection: (threadId) =>
          threadId !== ThreadId.make("billing")
            ? Effect.fail("unreadable")
            : Effect.succeed({
                runs: [
                  { id: RunId.make("billing:run"), userMessageId: "billing:first" },
                  { id: RunId.make("billing:queued"), userMessageId: "billing:queued" },
                ],
                messages: [
                  { id: "billing:first", text: "Migrate the billing tables" },
                  { id: "billing:queued", text: "Then add an index" },
                ],
              }),
      });
      const answer = yield* Queue.take(answers);
      assert.equal(answer.threadId, heldThreadId);
      // a thread whose history cannot be read is still named, without its request
      assert.equal(
        answer.context,
        [
          "Other active work in this project:",
          "- Billing (branch billing-tables): Migrate the billing tables",
          "- Docs",
        ].join("\n"),
      );
    }).pipe(Effect.scoped),
  );

  it.effect("answers without context before the deadline when the project read stalls", () =>
    Effect.gen(function* () {
      const reading = yield* Deferred.make<void>();
      const answers = yield* fakeConnection({
        shell: Deferred.succeed(reading, undefined).pipe(Effect.andThen(Effect.never)),
      });
      yield* Deferred.await(reading);
      // the answer leaves a second before the ten second deadline
      yield* TestClock.adjust("8999 millis");
      assert.isTrue(Option.isNone(yield* Queue.poll(answers)));
      yield* TestClock.adjust("1 millis");
      const answer = yield* Queue.take(answers);
      assert.notProperty(answer, "context");
    }).pipe(Effect.scoped),
  );

  it.effect("answers at once without context when the project cannot be read", () =>
    Effect.gen(function* () {
      const answers = yield* fakeConnection({ shell: Effect.fail("connection lost") });
      const answer = yield* Queue.take(answers);
      assert.equal(answer.threadId, heldThreadId);
      assert.notProperty(answer, "context");
    }).pipe(Effect.scoped),
  );
});
