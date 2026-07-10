import { readFile, readdir, stat, writeFile, mkdir } from "node:fs/promises";
import { dirname, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const webRoot = resolve(scriptDir, "..");
const repositoryRoot = resolve(webRoot, "..");
const outputPath = join(webRoot, "app", "data", "content.generated.json");
const config = JSON.parse(
  await readFile(join(webRoot, "content.config.json"), "utf8"),
);

const ignoredDirectories = new Set([
  ".git",
  ".github",
  ".agents",
  ".codex",
  "node_modules",
  "web",
]);
const categoryLabels = {
  concepts: "개념",
  troubleshooting: "트러블슈팅",
  commands: "명령어",
  examples: "실습",
};
const categoryOrder = {
  concepts: 0,
  commands: 1,
  examples: 2,
  troubleshooting: 3,
};
const fallbackColors = [
  ["#7668ed", "#efedff"],
  ["#0e9f7b", "#e3f7f1"],
  ["#d67d16", "#fff0dc"],
];

function toPosix(pathname) {
  return pathname.split(sep).join("/");
}

function extractTitle(markdown, fallback) {
  return markdown.match(/^#\s+(.+)$/m)?.[1]?.trim() ?? fallback;
}

function stripMarkdown(value) {
  return value
    .replace(/\[([^\]]+)\]\([^)]+\)/g, "$1")
    .replace(/[`*_>#|]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function extractSummary(markdown) {
  const withoutCode = markdown.replace(/```[\s\S]*?```/g, "");
  const paragraph = withoutCode
    .split(/\n\s*\n/)
    .map((value) => value.trim())
    .find(
      (value) =>
        value.length > 25 &&
        !value.startsWith("#") &&
        !value.startsWith("-") &&
        !/^\d+\.\s/.test(value) &&
        !value.startsWith("|"),
    );
  const summary = stripMarkdown(paragraph ?? "핵심 개념과 운영 기준을 정리한 문서입니다.");
  return summary.length > 155 ? `${summary.slice(0, 152)}…` : summary;
}

function extractTags(title, markdown, defaults) {
  const source = `${title} ${markdown}`.toLowerCase();
  const keywords = [
    ["Network", ["network", "네트워크", "localhost"]],
    ["Storage", ["volume", "볼륨", "persistence", "aof", "rdb"]],
    ["Security", ["permission", "권한", "acl", "인증", "tls"]],
    ["Operations", ["운영", "monitor", "장애", "설정"]],
    ["CLI", ["명령어", "cli", "command"]],
    ["HA", ["sentinel", "cluster", "replica", "고가용성"]],
  ];
  const detected = keywords
    .filter(([, matches]) => matches.some((word) => source.includes(word)))
    .map(([label]) => label);
  return [...new Set([...detected, ...defaults])].slice(0, 3);
}

function getDifficulty(category, filename) {
  if (category === "troubleshooting") return "실전";
  if (category === "commands") return "참고";
  const sequence = Number.parseInt(filename.match(/^(\d+)/)?.[1] ?? "1", 10);
  return sequence >= 5 ? "중급" : "입문";
}

async function collectMarkdownFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    if (entry.name.startsWith(".")) continue;
    const absolutePath = join(directory, entry.name);
    if (entry.isDirectory()) {
      files.push(...(await collectMarkdownFiles(absolutePath)));
    } else if (entry.isFile() && entry.name.endsWith(".md")) {
      files.push(absolutePath);
    }
  }
  return files;
}

const rootEntries = await readdir(repositoryRoot, { withFileTypes: true });
const topicDirectories = [];
for (const entry of rootEntries) {
  if (!entry.isDirectory() || ignoredDirectories.has(entry.name)) continue;
  const readmePath = join(repositoryRoot, entry.name, "README.md");
  try {
    if ((await stat(readmePath)).isFile()) topicDirectories.push(entry.name);
  } catch {
    // A top-level folder becomes a topic only when it has a README index.
  }
}

const topics = [];
for (const [index, topicId] of topicDirectories.entries()) {
  const topicRoot = join(repositoryRoot, topicId);
  const readme = await readFile(join(topicRoot, "README.md"), "utf8");
  const topicConfig = config[topicId] ?? {};
  const colors = fallbackColors[index % fallbackColors.length];
  const markdownFiles = (await collectMarkdownFiles(topicRoot)).filter(
    (path) => relative(topicRoot, path) !== "README.md",
  );

  const documents = [];
  for (const absolutePath of markdownFiles) {
    const sourcePath = toPosix(relative(repositoryRoot, absolutePath));
    const relativePath = toPosix(relative(topicRoot, absolutePath));
    const category = relativePath.split("/")[0] ?? "concepts";
    const filename = relativePath.split("/").at(-1) ?? relativePath;
    const markdown = await readFile(absolutePath, "utf8");
    const title = extractTitle(markdown, filename.replace(/\.md$/, ""));
    const sequence = Number.parseInt(filename.match(/^(\d+)/)?.[1] ?? "999", 10);
    documents.push({
      id: sourcePath.replace(/\.md$/, ""),
      topicId,
      sourcePath,
      category,
      categoryLabel: categoryLabels[category] ?? "문서",
      title,
      summary: extractSummary(markdown),
      difficulty: getDifficulty(category, filename),
      readTime: Math.max(3, Math.ceil(stripMarkdown(markdown).length / 900)),
      tags: extractTags(title, markdown, topicConfig.tags ?? []),
      sections: [...markdown.matchAll(/^##\s+(.+)$/gm)].map((match) => match[1].trim()),
      order: (categoryOrder[category] ?? 9) * 1000 + sequence,
      body: markdown,
    });
  }

  documents.sort((left, right) => left.order - right.order || left.title.localeCompare(right.title, "ko"));
  topics.push({
    id: topicId,
    title: topicConfig.title ?? extractTitle(readme, topicId),
    kicker: topicConfig.kicker ?? "DEVOPS TOPIC",
    description: topicConfig.description ?? extractSummary(readme),
    accent: topicConfig.accent ?? colors[0],
    accentSoft: topicConfig.accentSoft ?? colors[1],
    order: topicConfig.order ?? index + 10,
    tags: topicConfig.tags ?? [],
    documents,
  });
}

topics.sort((left, right) => left.order - right.order || left.title.localeCompare(right.title, "ko"));
await mkdir(dirname(outputPath), { recursive: true });
await writeFile(
  outputPath,
  `${JSON.stringify({ generatedAt: new Date().toISOString(), topics }, null, 2)}\n`,
  "utf8",
);

const documentCount = topics.reduce((total, topic) => total + topic.documents.length, 0);
console.log(`Synced ${documentCount} documents across ${topics.length} topics.`);
