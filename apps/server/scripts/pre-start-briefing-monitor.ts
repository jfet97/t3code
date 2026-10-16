/**
 * A reference pre-start monitor. While it runs, the first message of a new thread in one
 * project waits until this monitor briefs it on the project's other active threads (their
 * titles, branches and current requests), or answers with no context when there are none.
 *
 * It reads a one-time pairing link from `T3_PAIRING_URL` (see docs/user/pre-start-monitors.md)
 * so the link stays out of process listings, asks only for orchestration read and operate
 * access, and never prints the link or the access token.
 *
 *   node apps/server/scripts/pre-start-briefing-monitor.ts --project /path/to/repo
 */
import * as NodeRuntime from "@effect/platform-node/NodeRuntime";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AuthAccessTokenType,
  AuthEnvironmentBootstrapTokenType,
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthTokenExchangeGrantType,
  EnvironmentHttpApi,
  ORCHESTRATION_PROTOCOL_QUERY_PARAM,
  ORCHESTRATION_PROTOCOL_VERSION,
  ORCHESTRATION_V2_WS_METHODS,
  WsRpcGroup,
  type OrchestrationV2PreStartStreamItem,
  type OrchestrationProjectShell,
  type OrchestrationV2ShellSnapshot,
  type ProjectId,
} from "@t3tools/contracts";
import { encodeOAuthScope } from "@t3tools/shared/oauthScope";
import * as Config from "effect/Config";
import * as Console from "effect/Console";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { Command, Flag } from "effect/cli";
import { FetchHttpClient, HttpClient } from "effect/http";
import * as HttpApiClient from "effect/http-api/HttpApiClient";
import * as RpcClient from "effect/rpc/RpcClient";
import * as RpcSerialization from "effect/rpc/RpcSerialization";
import * as Socket from "effect/socket/Socket";

class PreStartMonitorSetupError extends Schema.TaggedError<PreStartMonitorSetupError>()(
  "PreStartMonitorSetupError",
  { detail: Schema.String },
) {
  override get message(): string {
    return this.detail;
  }
}

const makeMonitorClient = RpcClient.make(WsRpcGroup);
export type PreStartMonitorClient =
  typeof makeMonitorClient extends Effect.Effect<infer Client, any, any> ? Client : never;

type HeldRun = Extract<OrchestrationV2PreStartStreamItem, { readonly kind: "held" }>;

const MAX_BRIEFED_THREADS = 10;
const MAX_REQUEST_CHARACTERS = 280;
// the answer itself still has to reach the server before the deadline
const ANSWER_MARGIN_MS = 1_000;

const oneLine = (text: string, limit: number) => {
  const flat = text.replace(/\s+/g, " ").trim();
  return flat.length <= limit ? flat : `${flat.slice(0, limit - 1)}…`;
};

/** Another thread's current work, as the briefing describes it. */
interface BriefedThread {
  readonly title: string;
  readonly branch: string | null;
  readonly request: string | undefined;
}

/** A briefing on the project's other active threads, or none when there are none. */
function projectBriefing(threads: ReadonlyArray<BriefedThread>): string | undefined {
  if (threads.length === 0) return undefined;
  const lines = threads.map((thread) => {
    const branch = thread.branch === null ? "" : ` (branch ${oneLine(thread.branch, 120)})`;
    const request =
      thread.request === undefined ? "" : `: ${oneLine(thread.request, MAX_REQUEST_CHARACTERS)}`;
    return `- ${oneLine(thread.title, 120)}${branch}${request}`;
  });
  return ["Other active work in this project:", ...lines].join("\n");
}

const activeProjectThreads = (
  snapshot: Pick<OrchestrationV2ShellSnapshot, "threads">,
  held: Pick<HeldRun, "projectId" | "threadId">,
) =>
  snapshot.threads
    .filter(
      (thread) =>
        thread.projectId === held.projectId &&
        thread.id !== held.threadId &&
        thread.archivedAt === null &&
        thread.activityRunStatus != null,
    )
    .slice(0, MAX_BRIEFED_THREADS);

/** Opens the RPC client the monitor talks through, for as long as the scope stays open. */
export const connectMonitorClient = (socketUrl: string) =>
  Layer.build(
    Layer.effect(
      RpcClient.Protocol,
      RpcClient.makeProtocolSocket({
        retryTransientErrors: false,
        retryPolicy: Schedule.recurs(0),
      }),
    ).pipe(
      Layer.provide(
        Layer.mergeAll(
          Socket.layerWebSocket(socketUrl).pipe(
            Layer.provide(Socket.layerWebSocketConstructorGlobal),
          ),
          RpcSerialization.layerJson,
        ),
      ),
    ),
  ).pipe(Effect.flatMap((context) => makeMonitorClient.pipe(Effect.provide(context))));

const latestSnapshot = (client: PreStartMonitorClient) =>
  client[ORCHESTRATION_V2_WS_METHODS.subscribeShell]({}).pipe(
    Stream.filterMap((item) =>
      item.kind === "snapshot" ? Result.succeed(item.snapshot) : Result.failVoid,
    ),
    Stream.runHead,
  );

/** What each of the project's other active threads is working on now. */
const readBriefing = (client: PreStartMonitorClient, held: HeldRun) =>
  Effect.gen(function* () {
    const snapshot = yield* latestSnapshot(client);
    const active = Option.isNone(snapshot) ? [] : activeProjectThreads(snapshot.value, held);
    const briefed = yield* Effect.forEach(
      active,
      (thread) =>
        client[ORCHESTRATION_V2_WS_METHODS.getThreadProjection]({ threadId: thread.id }).pipe(
          Effect.map((projection) => {
            // the active run's message is the current request; later ones are only queued
            const run = projection.runs.find((candidate) => candidate.id === thread.activeRunId);
            return projection.messages.find((message) => message.id === run?.userMessageId)?.text;
          }),
          // a thread that cannot be read is still briefed, without its request
          Effect.orElseSucceed(() => undefined),
          Effect.map((request): BriefedThread => ({
            title: thread.title,
            branch: thread.branch,
            request,
          })),
        ),
      { concurrency: 4 },
    );
    return projectBriefing(briefed);
  });

/**
 * Monitors `projectId` until the connection ends. Each held first message is answered
 * before its deadline: with a briefing, or with none when the briefing fails or runs late.
 */
export const monitorProject = Effect.fn("preStartBriefingMonitor.monitorProject")(function* (
  client: PreStartMonitorClient,
  input: {
    readonly projectId: ProjectId;
    readonly holdMs?: number | undefined;
    /** Runs once new first messages in the project are held for this monitor. */
    readonly onReady?: Effect.Effect<void> | undefined;
  },
) {
  const subscription = yield* Deferred.make<string>();
  const brief = Effect.fn("preStartBriefingMonitor.brief")(function* (held: HeldRun) {
    const subscriptionId = yield* Deferred.await(subscription);
    const budgetMs =
      DateTime.toEpochMillis(DateTime.makeUnsafe(held.expiresAt)) -
      DateTime.toEpochMillis(yield* DateTime.now) -
      ANSWER_MARGIN_MS;
    const context =
      budgetMs <= 0
        ? undefined
        : yield* readBriefing(client, held).pipe(
            Effect.timeoutOption(budgetMs),
            Effect.map(Option.getOrUndefined),
            Effect.catchCause((cause) =>
              Console.error(`Could not read the project for thread ${held.threadId}.`, cause).pipe(
                Effect.as(undefined),
              ),
            ),
          );
    const answer = yield* client[ORCHESTRATION_V2_WS_METHODS.respondPreStart]({
      subscriptionId,
      threadId: held.threadId,
      runId: held.runId,
      requestId: held.requestId,
      ...(context === undefined ? {} : { context }),
    });
    yield* Console.log(
      answer.accepted
        ? `Briefed thread ${held.threadId}${context === undefined ? " without context" : ""}.`
        : `Thread ${held.threadId} no longer waited for a briefing.`,
    );
  });
  yield* client[ORCHESTRATION_V2_WS_METHODS.subscribePreStart]({
    projectId: input.projectId,
    ...(input.holdMs === undefined ? {} : { holdMs: input.holdMs }),
  }).pipe(
    // one slow briefing must not hold back the next thread's
    Stream.mapEffect(
      (item) =>
        item.kind === "ready"
          ? Deferred.succeed(subscription, item.subscriptionId).pipe(
              Effect.andThen(Console.log(`Monitoring new threads in ${input.projectId}.`)),
              Effect.andThen(input.onReady ?? Effect.void),
            )
          : brief(item).pipe(
              Effect.catchCause((cause) =>
                Console.error(
                  `Could not answer thread ${item.threadId}; it continues at its deadline.`,
                  cause,
                ),
              ),
            ),
      { concurrency: "unbounded" },
    ),
    Stream.runDrain,
  );
});

/** The project `query` names by id, workspace root or title; ambiguous names are refused. */
export function matchProject(
  projects: ReadonlyArray<Pick<OrchestrationProjectShell, "id" | "workspaceRoot" | "title">>,
  query: string,
): { readonly id: ProjectId } | { readonly problem: string } {
  const byId = projects.find((project) => project.id === query);
  if (byId !== undefined) return { id: byId.id };
  for (const matches of [
    projects.filter((project) => project.workspaceRoot === query),
    projects.filter((project) => project.title === query),
  ]) {
    if (matches.length > 1) return { problem: `Several projects match ${query}; pass its id.` };
    const [match] = matches;
    if (match !== undefined) return { id: match.id };
  }
  return { problem: `No project matches ${query}.` };
}

/** Exchanges the one-time pairing URL for a WebSocket URL with a short-lived ticket. */
const authorizeSocketUrl = Effect.fn("preStartBriefingMonitor.authorize")(function* (
  pairingUrl: Redacted.Redacted<string>,
) {
  const pairing = new URL(Redacted.value(pairingUrl));
  const credential = new URLSearchParams(pairing.hash.slice(1)).get("token");
  if (credential === null || credential === "") {
    return yield* new PreStartMonitorSetupError({
      detail: "T3_PAIRING_URL must be a pairing URL with a #token.",
    });
  }
  const auth = yield* HttpApiClient.group(EnvironmentHttpApi, {
    httpClient: yield* HttpClient.HttpClient,
    group: "auth",
    baseUrl: `${pairing.origin}/`,
  });
  // auth failures keep only their tag, so the credential and token never reach the output
  const failed = (step: string) => (cause: { readonly _tag: string }) =>
    new PreStartMonitorSetupError({ detail: `Could not ${step} (${cause._tag}).` });
  const access = yield* auth
    .token({
      headers: {},
      payload: {
        grant_type: AuthTokenExchangeGrantType,
        subject_token: credential,
        subject_token_type: AuthEnvironmentBootstrapTokenType,
        requested_token_type: AuthAccessTokenType,
        scope: encodeOAuthScope([AuthOrchestrationReadScope, AuthOrchestrationOperateScope]),
        client_label: "Pre-start briefing monitor",
      },
    })
    .pipe(Effect.mapError(failed("pair with the environment; create a fresh pairing link")));
  const issued = yield* auth
    .webSocketTicket({ headers: { authorization: `Bearer ${access.access_token}` } })
    .pipe(Effect.mapError(failed("open a connection to the environment")));
  const socket = new URL("/ws", pairing.origin);
  socket.protocol = pairing.protocol === "https:" ? "wss:" : "ws:";
  socket.searchParams.set("wsTicket", issued.ticket);
  socket.searchParams.set(
    ORCHESTRATION_PROTOCOL_QUERY_PARAM,
    String(ORCHESTRATION_PROTOCOL_VERSION),
  );
  return socket.toString();
});

const resolveProject = Effect.fn("preStartBriefingMonitor.resolveProject")(function* (
  client: PreStartMonitorClient,
  project: string,
) {
  const snapshot = yield* latestSnapshot(client);
  const match = matchProject(Option.getOrUndefined(snapshot)?.projects ?? [], project);
  if ("problem" in match) return yield* new PreStartMonitorSetupError({ detail: match.problem });
  return match.id;
});

/** Pairs with the environment, then briefs new threads in `project` until interrupted. */
export const runBriefingMonitor = Effect.fn("preStartBriefingMonitor.run")(function* (input: {
  readonly pairingUrl: Redacted.Redacted<string>;
  readonly project: string;
  readonly holdMs?: number | undefined;
  readonly onReady?: Effect.Effect<void> | undefined;
}) {
  const client = yield* connectMonitorClient(yield* authorizeSocketUrl(input.pairingUrl));
  yield* monitorProject(client, {
    projectId: yield* resolveProject(client, input.project),
    holdMs: input.holdMs,
    onReady: input.onReady,
  });
});

const monitorCommand = Command.make(
  "pre-start-briefing-monitor",
  {
    project: Flag.String("project").pipe(
      Flag.withDescription("Project id, workspace root or title to monitor."),
    ),
    holdSeconds: Flag.Int("hold-seconds").pipe(
      Flag.withDescription(
        "Seconds a new thread waits for the briefing (10 by default, at most 120).",
      ),
      Flag.optional,
    ),
  },
  ({ project, holdSeconds }) =>
    Config.Redacted("T3_PAIRING_URL").pipe(
      Effect.flatMap((pairingUrl) =>
        runBriefingMonitor({
          pairingUrl,
          project,
          holdMs: Option.getOrUndefined(Option.map(holdSeconds, (seconds) => seconds * 1_000)),
        }),
      ),
      Effect.scoped,
    ),
).pipe(Command.withDescription("Brief new threads in one project on its other active work."));

if (import.meta.main) {
  Command.run(monitorCommand, { version: "0.0.0" }).pipe(
    Effect.provide([NodeServices.layer, FetchHttpClient.layer]),
    NodeRuntime.runMain,
  );
}
