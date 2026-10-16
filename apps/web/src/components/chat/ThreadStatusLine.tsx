import type { ReactNode } from "react";

import { InlineButton } from "../ui/button";

export function ThreadStatusLine(props: {
  readonly icon: ReactNode;
  readonly label: string;
  /** Text that stays visible when the label truncates, such as a deadline. */
  readonly detail?: string;
  readonly actionLabel: string;
  readonly actionDisabled?: boolean;
  readonly onAction: () => void;
}) {
  return (
    // When the line is too narrow the action wraps below, and then its own text, so the detail
    // and the action always stay in view
    <div
      data-chat-thread-status-line="true"
      className="mx-auto flex w-full min-w-0 max-w-(--chat-content-max-width) flex-wrap items-center gap-x-1.5 gap-y-0.5 px-1 pb-2 text-muted-foreground text-xs [--inline-button-text-align:start] [--inline-button-white-space:normal] [&_[data-slot=inline-button]]:max-w-full [&_svg]:size-3.5 [&_svg]:shrink-0"
    >
      <span className="flex min-w-0 items-center gap-1.5">
        {props.icon}
        <span className="min-w-0 truncate">{props.label}</span>
        {props.detail === undefined ? null : (
          <span className="shrink-0 tabular-nums">{props.detail}</span>
        )}
        <span aria-hidden="true">·</span>
      </span>
      <InlineButton tone="muted" disabled={props.actionDisabled} onClick={props.onAction}>
        {props.actionLabel}
      </InlineButton>
    </div>
  );
}
