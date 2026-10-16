# Pre-start monitors

A pre-start monitor is a program that sees the first message of each new thread
in one project before the agent starts, and can add context to it. While a
monitor of that project is connected, the first message waits for it: 10 seconds
by default, and up to two minutes if the monitor asks for more. Other projects,
later messages, and messages sent by agents, scheduled tasks, forks or slash
commands are never held.

## While a thread waits

The thread shows **Waiting for monitor until** and the time the monitor must
answer by. The agent starts when the monitor answers, when that time passes, or
when you choose **Continue without monitor**, and only once the thread's
workspace is ready. Stop ends the thread's first run as usual.

## Run the reference monitor

The T3 Code source includes a monitor that tells each new thread about the
project's other active threads: their titles, branches and current requests.

1. On the host, create a pairing link for it:

   ```sh
   npx t3 pair --label "Pre-start monitor" --scope orchestration:read --scope orchestration:operate
   ```

2. From a checkout of T3 Code, paste the link when prompted and start the
   monitor. Reading the link this way keeps it out of your shell history and of
   the process list:

   ```sh
   read -rs T3_PAIRING_URL && export T3_PAIRING_URL
   node apps/server/scripts/pre-start-briefing-monitor.ts --project /path/to/repo
   ```

   `--project` takes the project's folder, title or id. `--hold-seconds` lets
   new threads wait longer for the briefing, up to 120.

The link works once. The monitor then appears under **Settings → Connections**,
where you can revoke it.

## Write your own

A monitor connects to the environment's WebSocket API, subscribes with
`orchestration.subscribePreStart` for one project, and answers each held message
with `orchestration.respondPreStart`. Both need permission to operate threads,
which is not limited to the project the monitor watches. The first answer before
the deadline wins.
