# Repository Guidelines

## Project Structure & Module Organization

This repository is a Korean-language DevOps knowledge base. Content is grouped by technology at the repository root:

- `docker/` contains concepts, CLI references, minimal examples, and troubleshooting notes.
- `redis/` contains operational concepts and configuration guidance.
- Each topic begins with a `README.md` that acts as its index and recommended reading order.

Within a topic, place conceptual material in `concepts/`, incident-style guides in `troubleshooting/`, command references in `commands/`, and reproducible demonstrations in `examples/`. Number ordered concept files with two digits, for example `concepts/03-replication.md`. Add new top-level topics only when they represent a distinct DevOps technology or practice.

## Build, Test, and Development Commands

Markdown has no build step. `web/` uses Node.js 22+ and npm. Before submitting, run:

```bash
git diff --check       # detect whitespace errors
cd web
npm run sync-content  # rebuild the generated content catalog
npm run lint          # check React and TypeScript source
npm test              # build and run rendered HTML tests
```

Run every command or configuration example you change when the required tool is available. Keep examples minimal and safe to copy into a local environment.

## Coding Style & Naming Conventions

Write concise Korean prose consistent with the existing notes. Use ATX headings (`#`, `##`), fenced code blocks with language tags such as `bash` or `yaml`, and relative links between repository documents. Prefer lowercase kebab-case filenames (`container-exits-immediately.md`) and two-space YAML indentation. Explain why a setting matters, not only how to type it. Troubleshooting documents should follow: symptom, cause, verification, and resolution.

## Testing Guidelines

Documentation has no coverage threshold; preview headings and lists, follow changed links, and confirm commands against the documented tool version. For `web/` changes, run `npm test` and verify search, topic navigation, code-copy controls, and responsive layouts. Examples should be reproducible from their own directory and include cleanup steps where resources persist.

## Commit & Pull Request Guidelines

The current history uses a short, imperative, capitalized subject (for example, `Add DevOps notes for Docker and Redis`). Follow that style and keep each commit focused on one topic. Pull requests should summarize the purpose, list affected paths, note how commands and links were verified, and link relevant issues. Include screenshots only when rendered layout or diagrams materially changed.

## Security & Configuration Tips

Never commit credentials, tokens, private hostnames, or production data. Use placeholders such as `${REDIS_PASSWORD}` and label destructive or environment-specific commands clearly.
