# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

Korean-language DevOps knowledge base with topic directories (`docker/`, `redis/`, `kafka/`) at the root, plus a Next.js web app (`web/`) that renders the Markdown content as a learning site.

## Commands

All web commands run from `web/`:

```bash
cd web
npm install              # install dependencies (Node.js 22+)
npm run sync-content     # rebuild app/data/content.generated.json from root Markdown
npm run dev              # local dev server (runs sync-content automatically)
npm run build            # production build (runs sync-content automatically)
npm test                 # build + rendered HTML tests
npm run lint             # ESLint for TypeScript/React
```

Pre-submit checklist:

```bash
git diff --check         # whitespace errors
cd web && npm run lint && npm test
```

## Content Pipeline

`web/scripts/sync-content.mjs` scans root directories that have a `README.md`, reads their Markdown files, and writes `web/app/data/content.generated.json`. This file is generated and should not be edited by hand.

Adding a new topic: create a top-level directory with `README.md` and subdirectories from `concepts/`, `troubleshooting/`, `commands/`, `examples/` as needed. Optionally add display metadata in `web/content.config.json`. The sync script picks it up automatically.

## Writing Conventions

- All prose in Korean. ATX headings, fenced code blocks with language tags, relative links.
- Filenames: lowercase kebab-case (e.g. `container-exits-immediately.md`).
- Ordered concept files: two-digit prefix (e.g. `03-persistence-backup-and-recovery.md`).
- Troubleshooting format: symptom, cause, verification, resolution.
- YAML indentation: two spaces.
- Explain *why* a setting matters, not just how to set it.

## Commit Style

Short, imperative, capitalized subject (e.g. `Add Kafka configuration reference note`). One commit per topic.

## Web Stack

Next.js 16 + React 19 + Vite + Tailwind CSS 4, deployed to Cloudflare Workers via `vinext`. TypeScript source in `web/app/`, build plugins in `web/build/`.
