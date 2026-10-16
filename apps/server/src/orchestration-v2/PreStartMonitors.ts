import type {
  AuthSessionId,
  OrchestrationV2PreStartStreamItem,
  OrchestrationV2Run,
  OrchestrationV2RunPreStart,
  ProjectId,
} from "@t3tools/contracts";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Stream from "effect/Stream";

import * as EventSink from "./EventSink.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const DEFAULT_HOLD_MS = 10_000;
const MAX_HOLD_MS = 120_000;

/**
 * Connections subscribed as pre-start monitors of one project each. While one is subscribed,
 * a new thread's first message in that project waits before its provider turn starts (see
 * `OrchestrationV2RunPreStart`), and every subscription of that project hears about it.
 */
export class PreStartMonitors extends Context.Service<
  PreStartMonitors,
  {
    /** How long a first run held now in `projectId` waits; none without a monitor of it. */
    readonly holdDuration: (
      projectId: ProjectId,
    ) => Effect.Effect<Option.Option<Duration.Duration>>;
    /** Whether `subscriptionId` is still open, belongs to `sessionId` and monitors `projectId`. */
    readonly isSubscribed: (input: {
      readonly sessionId: AuthSessionId;
      readonly subscriptionId: string;
      readonly projectId: ProjectId;
    }) => Effect.Effect<boolean>;
    /** A ready frame, then every first run held in `projectId` until the stream ends. */
    readonly subscribe: (input: {
      readonly sessionId: AuthSessionId;
      readonly projectId: ProjectId;
      readonly holdMs?: number | undefined;
    }) => Stream.Stream<
      OrchestrationV2PreStartStreamItem,
      EventSink.EventSinkV2Error | ProjectionStore.ProjectionStoreV2Error
    >;
  }
>()("t3/orchestration-v2/PreStartMonitors") {}

const make = Effect.gen(function* () {
  const eventSink = yield* EventSink.EventSinkV2;
  const projectionStore = yield* ProjectionStore.ProjectionStoreV2;
  const subscriptions = yield* Ref.make<
    ReadonlyMap<
      string,
      {
        readonly sessionId: AuthSessionId;
        readonly projectId: ProjectId;
        readonly holdMs: number;
      }
    >
  >(new Map());

  const holdDuration: PreStartMonitors["Service"]["holdDuration"] = (projectId) =>
    Ref.get(subscriptions).pipe(
      Effect.map((current) => {
        const budgets = Array.from(current.values()).flatMap((subscription) =>
          subscription.projectId === projectId ? [subscription.holdMs] : [],
        );
        return budgets.length === 0
          ? Option.none()
          : Option.some(Duration.millis(Math.max(...budgets)));
      }),
    );

  const isSubscribed: PreStartMonitors["Service"]["isSubscribed"] = (input) =>
    Ref.get(subscriptions).pipe(
      Effect.map((current) => {
        const subscription = current.get(input.subscriptionId);
        return (
          subscription?.sessionId === input.sessionId && subscription.projectId === input.projectId
        );
      }),
    );

  const heldRun = (
    run: OrchestrationV2Run,
    preStart: OrchestrationV2RunPreStart,
    projectId: ProjectId,
  ) =>
    projectionStore
      .getThreadRecords(run.threadId, ["messages"], { messageIds: [run.userMessageId] })
      .pipe(
        Effect.map((records): Option.Option<OrchestrationV2PreStartStreamItem> =>
          records.thread.projectId !== projectId
            ? Option.none()
            : Option.some({
                kind: "held",
                projectId,
                threadId: run.threadId,
                runId: run.id,
                messageId: run.userMessageId,
                requestId: preStart.requestId,
                expiresAt: preStart.expiresAt,
                text: records.messages[0]?.text ?? "",
              }),
        ),
      );

  const subscribe: PreStartMonitors["Service"]["subscribe"] = (input) =>
    Stream.unwrap(
      Effect.gen(function* () {
        // read before registering, so the stream replays a run held in between
        const afterSequence = yield* eventSink.latestSequence();
        const subscriptionId = yield* randomUuidV4;
        yield* Effect.acquireRelease(
          Ref.update(subscriptions, (current) =>
            new Map(current).set(subscriptionId, {
              sessionId: input.sessionId,
              projectId: input.projectId,
              holdMs: Math.min(MAX_HOLD_MS, input.holdMs ?? DEFAULT_HOLD_MS),
            }),
          ),
          () =>
            Ref.update(subscriptions, (current) => {
              const next = new Map(current);
              next.delete(subscriptionId);
              return next;
            }),
        );
        const held = eventSink
          .stream({ afterSequence, eventType: "run.created", bounded: true })
          .pipe(
            Stream.filterMap((stored) =>
              stored.event.type === "run.created" &&
              stored.event.payload.preStart?.status === "pending"
                ? Result.succeed({
                    run: stored.event.payload,
                    preStart: stored.event.payload.preStart,
                  })
                : Result.failVoid,
            ),
            Stream.mapEffect(({ run, preStart }) => heldRun(run, preStart, input.projectId)),
            Stream.filterMap((item) =>
              Option.isSome(item) ? Result.succeed(item.value) : Result.failVoid,
            ),
          );
        return Stream.concat(
          Stream.succeed<OrchestrationV2PreStartStreamItem>({ kind: "ready", subscriptionId }),
          held,
        );
      }),
    );

  return PreStartMonitors.of({ holdDuration, isSubscribed, subscribe });
});

export const layer = Layer.effect(PreStartMonitors, make);
