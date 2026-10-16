import type {
  AuthSessionId,
  OrchestrationV2PreStartStreamItem,
  OrchestrationV2Run,
  OrchestrationV2RunPreStart,
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

const DEFAULT_HOLD_MS = 120_000;
const MAX_HOLD_MS = 120_000;

/**
 * Connections subscribed as pre-start monitors. While one is subscribed, a new thread's
 * first message waits before its provider turn starts (see `OrchestrationV2RunPreStart`),
 * and every subscription hears about it.
 */
export class PreStartMonitors extends Context.Service<
  PreStartMonitors,
  {
    /** How long a first run held now waits; none while no monitor is subscribed. */
    readonly holdDuration: Effect.Effect<Option.Option<Duration.Duration>>;
    /** Whether `subscriptionId` is still open and belongs to `sessionId`. */
    readonly isSubscribed: (input: {
      readonly sessionId: AuthSessionId;
      readonly subscriptionId: string;
    }) => Effect.Effect<boolean>;
    /** A ready frame, then every first run held from then on, until the stream ends. */
    readonly subscribe: (input: {
      readonly sessionId: AuthSessionId;
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
    ReadonlyMap<string, { readonly sessionId: AuthSessionId; readonly holdMs: number }>
  >(new Map());

  const holdDuration = Ref.get(subscriptions).pipe(
    Effect.map((current) =>
      current.size === 0
        ? Option.none()
        : Option.some(
            Duration.millis(
              Math.max(...Array.from(current.values(), (subscription) => subscription.holdMs)),
            ),
          ),
    ),
  );

  const isSubscribed: PreStartMonitors["Service"]["isSubscribed"] = (input) =>
    Ref.get(subscriptions).pipe(
      Effect.map((current) => current.get(input.subscriptionId)?.sessionId === input.sessionId),
    );

  const heldRun = (run: OrchestrationV2Run, preStart: OrchestrationV2RunPreStart) =>
    projectionStore
      .getThreadRecords(run.threadId, ["messages"], { messageIds: [run.userMessageId] })
      .pipe(
        Effect.map((records): OrchestrationV2PreStartStreamItem => ({
          kind: "held",
          threadId: run.threadId,
          runId: run.id,
          messageId: run.userMessageId,
          requestId: preStart.requestId,
          expiresAt: preStart.expiresAt,
          text: records.messages[0]?.text ?? "",
        })),
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
            Stream.mapEffect(({ run, preStart }) => heldRun(run, preStart)),
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
