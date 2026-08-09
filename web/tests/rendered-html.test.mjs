import assert from "node:assert/strict";
import { access, readFile } from "node:fs/promises";
import test from "node:test";

const projectRoot = new URL("../", import.meta.url);

async function render(pathname = "/") {
  const workerUrl = new URL("../dist/server/index.js", import.meta.url);
  workerUrl.searchParams.set("test", `${process.pid}-${Date.now()}`);
  const { default: worker } = await import(workerUrl.href);

  return worker.fetch(
    new Request(`http://localhost${pathname}`, {
      headers: { accept: "text/html" },
    }),
    {
      ASSETS: {
        fetch: async () => new Response("Not found", { status: 404 }),
      },
    },
    {
      waitUntil() {},
      passThroughOnException() {},
    },
  );
}

test("server-renders the DevOps learning home", async () => {
  const response = await render();
  assert.equal(response.status, 200);
  assert.match(response.headers.get("content-type") ?? "", /^text\/html\b/i);

  const html = await response.text();
  assert.match(html, /<html lang="ko">/);
  assert.match(html, /<title>DevOps Note — 운영 지식을 내 것으로<\/title>/);
  assert.match(html, /운영 지식을/);
  assert.match(html, /주제별로 차근차근/);
  assert.match(html, /문제에서 시작하는 학습/);
  assert.match(html, /Docker/);
  assert.match(html, /Redis/);
  assert.match(html, /Redis memory pressure와 OOM/);
  assert.match(html, /property="og:image" content="http:\/\/localhost(?::3000)?\/og\.png"/);
  assert.doesNotMatch(html, /codex-preview|Your site is taking shape|react-loading-skeleton/);
});

test("keeps the generated content catalog extensible", async () => {
  const [catalogSource, page, syncScript, packageSource] = await Promise.all([
    readFile(new URL("../app/data/content.generated.json", import.meta.url), "utf8"),
    readFile(new URL("../app/page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../scripts/sync-content.mjs", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);
  const catalog = JSON.parse(catalogSource);
  const topicIds = catalog.topics.map((topic) => topic.id);
  const documentCount = catalog.topics.reduce(
    (total, topic) => total + topic.documents.length,
    0,
  );

  assert.deepEqual(topicIds, ["docker", "redis", "kafka", "aws"]);
  assert.ok(documentCount >= 22);
  assert.ok(catalog.topics.every((topic) => topic.documents.length > 0));
  assert.ok(
    catalog.topics
      .flatMap((topic) => topic.documents)
      .some((document) => document.id === "redis/concepts/01-data-model-and-expiration"),
  );
  assert.match(page, /devops-note-completed/);
  assert.match(page, /MarkdownDocument/);
  assert.match(syncScript, /README\.md/);
  assert.match(packageSource, /"sync-content": "node scripts\/sync-content\.mjs"/);
  assert.doesNotMatch(packageSource, /react-loading-skeleton/);

  await access(new URL("../public/og.png", import.meta.url));
  await assert.rejects(access(new URL("app/_sites-preview/SkeletonPreview.tsx", projectRoot)));
  await assert.rejects(access(new URL("app/_sites-preview/preview.css", projectRoot)));
});
