# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Korean-language DevOps knowledge base. Topic directories (`docker/`, `redis/`, `kafka/`) at the root hold the Markdown content; a Next.js app in `web/` renders it as a searchable learning site.

`AGENTS.md` covers the same ground for other agents — keep the two in sync when you change conventions here.

## Commands

All web commands run from `web/` (Node.js 22+):

```bash
cd web
npm install              # install dependencies
npm run sync-content     # rebuild app/data/content.generated.json from root Markdown
npm run dev              # local dev server (runs sync-content first)
npm run build            # production build (runs sync-content first)
npm test                 # build, then node --test on the rendered HTML
npm run lint             # ESLint over web/ (dist and .next ignored)
```

There is no single-test runner — `npm test` builds and runs both cases in `tests/rendered-html.test.mjs`. That test imports the *built* worker (`dist/server/index.js`) and calls `worker.fetch`, so it only reflects source changes after a build (hence the `test` script builds first). It asserts on literal page strings and topic ids, so content edits can break it.

Pre-submit checklist:

```bash
git diff --check         # whitespace errors
cd web && npm run lint && npm test
```

## Content Pipeline (spans multiple files)

`web/scripts/sync-content.mjs` scans the repo root, treats every directory that has a `README.md` as a topic (skipping `.git`, `.github`, `.agents`, `.codex`, `node_modules`, `web`), and writes `web/app/data/content.generated.json`. That JSON is imported directly by `web/app/page.tsx`. The file is generated — never edit it by hand.

The sync script *derives* per-document metadata from structure, so filenames and folders are load-bearing:

- **Category** = first path segment under the topic (`concepts/`, `troubleshooting/`, `commands/`, `examples/`) → sets the Korean label and ordering group.
- **Ordering** = category order × 1000 + the two-digit filename prefix; a file with no numeric prefix sorts last (999).
- **Difficulty** is inferred: `troubleshooting` → `실전`, `commands` → `참고`, else numeric prefix `≥ 5` → `중급`, else `입문`.
- **Title** = first `# H1`. **Summary** = first substantial non-heading/list/table paragraph. **Sections** (in-page TOC) = every `## H2`. **Tags** = keyword match over title+body merged with `content.config.json` defaults.

So renaming a file, dropping its numeric prefix, or moving it between category folders silently changes its sort order, difficulty badge, and label — not just its URL.

`web/content.config.json` supplies per-topic display metadata (title, kicker, accent colors, order, tags). A topic renders without an entry (colors/label fall back), but add one for anything user-facing.

Adding a topic: create a root directory with `README.md` plus any of `concepts/`, `troubleshooting/`, `commands/`, `examples/`, optionally a `content.config.json` block. Sync picks it up automatically.

## Web Rendering (the other cross-file surface)

`web/app/page.tsx` is a single `"use client"` component holding the entire UI **and a hand-written Markdown renderer** — there is no Markdown library. It supports a deliberate subset: fenced code blocks, `##`/`###` headings (`#` is dropped since the page renders the title itself), `-`/`*` and ordered lists (no nesting), `>` blockquotes, pipe tables, paragraphs; inline `` `code` ``, `**bold**`, `[text](link)`, and bare `http` URLs. Syntax outside this subset won't render as expected — check new syntax in the browser.

Relative links between docs become in-app navigation: `normalizeDocumentLink` resolves a relative href against the current doc's `sourcePath` to a document `id`; if that id exists the link renders as a button that opens the doc in place. Keep cross-doc links pointing at real relative paths so they resolve.

## Deployment

Next.js 16 + React 19 + Vite + Tailwind CSS 4, served on Cloudflare Workers via `vinext`. `web/worker/index.ts` is the Worker entry (wraps vinext's app-router handler plus an image-optimization route). `web/build/sites-vite-plugin.ts` also packages an OpenAI "Sites" bundle (`.openai/hosting.json` + `drizzle/`) into `dist/.openai` after build. `web/db/` and `web/drizzle/` are template scaffolding — no application code uses them yet.

## Writing Conventions

- All prose in Korean. ATX headings, fenced code blocks with language tags, relative links.
- Filenames: lowercase kebab-case (e.g. `container-exits-immediately.md`).
- Ordered concept files: two-digit prefix (e.g. `03-persistence-backup-and-recovery.md`) — drives sort order and the difficulty badge.
- Troubleshooting format: symptom, cause, verification, resolution.
- YAML indentation: two spaces. Explain *why* a setting matters, not just how to set it.
- Placeholders for secrets/hosts (e.g. `${REDIS_PASSWORD}`); never commit real credentials or production hostnames.

## Commit Style

Short, imperative, capitalized subject (e.g. `Add Kafka configuration reference note`). One commit per topic.
