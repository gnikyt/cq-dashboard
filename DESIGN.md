# Design

Durable visual rules for the cq-dashboard web surface. Tokens live in
`web/templates/layout.html`; this file records the decisions behind them.

## Mode

**Operate.** The visitor is mid-task: checking queue health, triaging a
failure, or reading what a job did. Expression never obscures state, task, or a
familiar affordance. The register is the category canon — Grafana and Sidekiq's
UI are the craft bar — played straight, without irony or smuggled quirk.

The surface serves three arrivals equally: something is wrong (triage), passive
monitoring, and dev-time debugging. The overview is a router, not a
destination: the stat rail answers "is anything wrong" in one glance, and every
figure that could be wrong links to the page that explains it.

## Type

- One family for chrome (`--sans`, system stack), one for data (`--mono`,
  system mono). Monospace is for IDs, counts, durations, and keys only... never
  as a costume for "technical".
- Fixed px scale, not fluid: 11 (labels), 12 (hints), 13 (body/tables),
  15 (brand), 20 (page title), 22 (stat figures). The tight ratio is
  deliberate; product UI has too many type elements for brand-surface contrast.
- Numeric columns use `font-variant-numeric: tabular-nums` and right alignment,
  so digits line up down a column.

## Color

Restrained: two neutral layers (`--bg` page, `--panel` surface,`--panel-2` for
headers and hover), one blue accent for actions and current nav, and semantic
hues used only for state.

Both themes ship. The scene is a developer at a desk beside a terminal and an
editor, which is as often light as dark, so the surface follows
`prefers-color-scheme` rather than committing.

Status color is assigned by **group**, not by state, so nine states read as six
ideas:

| Group | States | Hue |
| --- | --- | --- |
| waiting | created, pending | neutral |
| running | active | accent blue |
| done | completed | green |
| failed | failed, cancelled | red |
| dropped | discarded | amber |
| shutdown | abandoned, interrupted | violet |

A count is neither good nor bad: tone colors a stat's caption, never the figure
itself. The exception is a failure count, where the figure is the alarm.

## Components

- **Panel**: bordered surface with a `panel-head` (title plus optional muted
  hint) and an optional `panel-foot` for a single explanatory line.
- **Stat**: uppercase label, large mono figure, one caption line. Links when
  the figure has a page behind it.
- **Badge**: status pill, colored by group, carrying a `title` with the state's
  one-line meaning. Every badge is a tooltip.
- **Table**: hairline separators, uppercase 11px headers, row hover, horizontal
  scroll inside the panel rather than the page.
- **Empty state**: a bold line naming the situation plus one line teaching what
  would fill it. Never a bare "nothing here".

Every interactive element has hover and a visible `:focus-visible` ring.

## Copy

Explanation belongs on the Reference page, not beside the data. A view may
carry one lede line and link to Reference for the caveat; panels never hold
paragraphs. Status meanings live in exactly one place (`web/status.go`) and are
rendered into both the badge tooltips and the Reference table.

## Motion

One authored moment: the live indicator's slow pulse, which conveys that the
page is polling. Transitions are 140ms on hover and focus. No page-load
choreography — the operator arrives mid-task. `prefers-reduced-motion` disables
the pulse.

## Constraints

No build step. Go `html/template` with one embedded stylesheet and a few lines
of inline JS for the live-region poll. No npm, no framework, no external fonts
or assets.
