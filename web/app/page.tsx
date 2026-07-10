"use client";

import { useEffect, useMemo, useState, type CSSProperties, type ReactNode } from "react";
import content from "./data/content.generated.json";

type DocumentEntry = {
  id: string;
  topicId: string;
  sourcePath: string;
  category: string;
  categoryLabel: string;
  title: string;
  summary: string;
  difficulty: string;
  readTime: number;
  tags: string[];
  sections: string[];
  order: number;
  body: string;
};

type Topic = {
  id: string;
  title: string;
  kicker: string;
  description: string;
  accent: string;
  accentSoft: string;
  order: number;
  tags: string[];
  documents: DocumentEntry[];
};

const topics = content.topics as Topic[];
const documents = topics.flatMap((topic) => topic.documents);
const topicMap = new Map(topics.map((topic) => [topic.id, topic]));
const documentMap = new Map(documents.map((document) => [document.id, document]));

function SearchIcon() {
  return <span aria-hidden="true" className="search-icon" />;
}

function ArrowIcon({ direction = "right" }: { direction?: "left" | "right" }) {
  return <span aria-hidden="true" className={`arrow-icon arrow-${direction}`} />;
}

function TopicMark({ topic, small = false }: { topic: Topic; small?: boolean }) {
  const style = {
    "--topic-accent": topic.accent,
    "--topic-soft": topic.accentSoft,
  } as CSSProperties;
  return (
    <span className={`topic-mark${small ? " topic-mark-small" : ""}`} style={style}>
      {topic.title.slice(0, 1)}
    </span>
  );
}

function useCompletion() {
  const [completed, setCompleted] = useState<Set<string>>(new Set());

  useEffect(() => {
    let next = new Set<string>();
    try {
      const saved = JSON.parse(localStorage.getItem("devops-note-completed") ?? "[]");
      next = new Set(Array.isArray(saved) ? saved : []);
    } catch {
      next = new Set();
    }
    const timer = window.setTimeout(() => setCompleted(next), 0);
    return () => window.clearTimeout(timer);
  }, []);

  function toggle(id: string) {
    setCompleted((current) => {
      const next = new Set(current);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      localStorage.setItem("devops-note-completed", JSON.stringify([...next]));
      return next;
    });
  }

  return { completed, toggle };
}

function normalizeDocumentLink(currentPath: string, href: string) {
  if (!href.endsWith(".md")) return null;
  const base = currentPath.split("/").slice(0, -1);
  for (const part of href.split("/")) {
    if (part === "..") base.pop();
    else if (part !== ".") base.push(part);
  }
  return base.join("/").replace(/\.md$/, "");
}

function InlineText({
  text,
  currentPath,
  onOpenDocument,
}: {
  text: string;
  currentPath: string;
  onOpenDocument: (id: string) => void;
}) {
  const tokens = text.split(/(`[^`]+`|\[[^\]]+\]\([^)]+\)|\*\*[^*]+\*\*|https?:\/\/[^\s)]+)/g);
  return (
    <>
      {tokens.map((token, index) => {
        if (token.startsWith("`") && token.endsWith("`")) {
          return <code key={index}>{token.slice(1, -1)}</code>;
        }
        const markdownLink = token.match(/^\[([^\]]+)\]\(([^)]+)\)$/);
        if (markdownLink) {
          const [, label, href] = markdownLink;
          const internalId = normalizeDocumentLink(currentPath, href);
          if (internalId && documentMap.has(internalId)) {
            return (
              <button className="inline-link" key={index} onClick={() => onOpenDocument(internalId)}>
                {label}
              </button>
            );
          }
          return (
            <a href={href} key={index} rel="noreferrer" target={href.startsWith("http") ? "_blank" : undefined}>
              {label}
            </a>
          );
        }
        if (token.startsWith("**") && token.endsWith("**")) {
          return <strong key={index}>{token.slice(2, -2)}</strong>;
        }
        if (token.startsWith("http")) {
          return <a href={token} key={index} rel="noreferrer" target="_blank">{token}</a>;
        }
        return token;
      })}
    </>
  );
}

function CodeBlock({ code, language }: { code: string; language: string }) {
  const [copied, setCopied] = useState(false);

  async function copyCode() {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1500);
  }

  return (
    <div className="code-block">
      <div className="code-toolbar">
        <span>{language || "text"}</span>
        <button onClick={copyCode}>{copied ? "복사됨" : "복사"}</button>
      </div>
      <pre><code>{code}</code></pre>
    </div>
  );
}

function slugify(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9가-힣]+/g, "-").replace(/(^-|-$)/g, "");
}

function MarkdownDocument({
  document,
  onOpenDocument,
}: {
  document: DocumentEntry;
  onOpenDocument: (id: string) => void;
}) {
  const lines = document.body.split("\n");
  const blocks: ReactNode[] = [];
  let index = 0;

  const inline = (text: string, key: number) => (
    <InlineText key={key} text={text} currentPath={document.sourcePath} onOpenDocument={onOpenDocument} />
  );

  while (index < lines.length) {
    const line = lines[index];
    if (!line.trim()) {
      index += 1;
      continue;
    }
    if (line.startsWith("```")) {
      const language = line.slice(3).trim();
      const code = [];
      index += 1;
      while (index < lines.length && !lines[index].startsWith("```")) {
        code.push(lines[index]);
        index += 1;
      }
      index += 1;
      blocks.push(<CodeBlock code={code.join("\n")} language={language} key={`code-${index}`} />);
      continue;
    }
    const heading = line.match(/^(#{1,3})\s+(.+)$/);
    if (heading) {
      const level = heading[1].length;
      const title = heading[2];
      if (level === 1) {
        index += 1;
        continue;
      }
      const Heading = level === 2 ? "h2" : "h3";
      blocks.push(<Heading id={slugify(title)} key={`heading-${index}`}>{inline(title, index)}</Heading>);
      index += 1;
      continue;
    }
    if (line.includes("|") && lines[index + 1]?.match(/^\s*\|?[\s:|-]+\|/)) {
      const rows = [];
      rows.push(line);
      index += 2;
      while (index < lines.length && lines[index].includes("|")) {
        rows.push(lines[index]);
        index += 1;
      }
      const parsedRows = rows.map((row) => row.split("|").map((cell) => cell.trim()).filter(Boolean));
      const [header, ...body] = parsedRows;
      blocks.push(
        <div className="table-wrap" key={`table-${index}`}>
          <table>
            <thead><tr>{header.map((cell, cellIndex) => <th key={cellIndex}>{inline(cell, cellIndex)}</th>)}</tr></thead>
            <tbody>{body.map((row, rowIndex) => <tr key={rowIndex}>{row.map((cell, cellIndex) => <td key={cellIndex}>{inline(cell, cellIndex)}</td>)}</tr>)}</tbody>
          </table>
        </div>,
      );
      continue;
    }
    if (/^[-*]\s+/.test(line)) {
      const items = [];
      while (index < lines.length && /^[-*]\s+/.test(lines[index])) {
        items.push(lines[index].replace(/^[-*]\s+/, ""));
        index += 1;
      }
      blocks.push(<ul key={`list-${index}`}>{items.map((item, itemIndex) => <li key={itemIndex}>{inline(item, itemIndex)}</li>)}</ul>);
      continue;
    }
    if (/^\d+\.\s+/.test(line)) {
      const items = [];
      while (index < lines.length && /^\d+\.\s+/.test(lines[index])) {
        items.push(lines[index].replace(/^\d+\.\s+/, ""));
        index += 1;
      }
      blocks.push(<ol key={`ordered-${index}`}>{items.map((item, itemIndex) => <li key={itemIndex}>{inline(item, itemIndex)}</li>)}</ol>);
      continue;
    }
    if (line.startsWith("> ")) {
      blocks.push(<blockquote key={`quote-${index}`}>{inline(line.slice(2), index)}</blockquote>);
      index += 1;
      continue;
    }
    const paragraph = [line];
    index += 1;
    while (
      index < lines.length &&
      lines[index].trim() &&
      !/^(#{1,3})\s+/.test(lines[index]) &&
      !lines[index].startsWith("```") &&
      !/^[-*]\s+/.test(lines[index]) &&
      !/^\d+\.\s+/.test(lines[index])
    ) {
      paragraph.push(lines[index]);
      index += 1;
    }
    blocks.push(<p key={`paragraph-${index}`}>{inline(paragraph.join(" "), index)}</p>);
  }

  return <div className="markdown-body">{blocks}</div>;
}

function Header({ onHome }: { onHome: () => void }) {
  return (
    <header className="site-header">
      <button className="brand" onClick={onHome} aria-label="DevOps Note 홈으로">
        <span className="brand-symbol" aria-hidden="true"><i /><i /><i /></span>
        <span><b>DEVOPS</b><small>NOTE</small></span>
      </button>
      <nav aria-label="주요 메뉴">
        <button onClick={() => { onHome(); window.setTimeout(() => document.querySelector("#tracks")?.scrollIntoView({ behavior: "smooth" }), 0); }}>학습 경로</button>
        <button onClick={() => { onHome(); window.setTimeout(() => document.querySelector("#troubleshooting")?.scrollIntoView({ behavior: "smooth" }), 0); }}>트러블슈팅</button>
        <span className="nav-status"><i /> 계속 업데이트 중</span>
      </nav>
    </header>
  );
}

function HomeView({
  completed,
  onOpenDocument,
}: {
  completed: Set<string>;
  onOpenDocument: (id: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [selectedTopic, setSelectedTopic] = useState("all");
  const normalizedQuery = query.trim().toLowerCase();
  const searchResults = useMemo(() => {
    if (!normalizedQuery) return [];
    return documents.filter((document) => {
      const haystack = `${document.title} ${document.summary} ${document.tags.join(" ")} ${document.body}`.toLowerCase();
      return haystack.includes(normalizedQuery) && (selectedTopic === "all" || document.topicId === selectedTopic);
    }).slice(0, 8);
  }, [normalizedQuery, selectedTopic]);
  const troubleshooting = documents.filter((document) => document.category === "troubleshooting");
  const firstTroubleshootingByTopic = topics.flatMap((topic) =>
    topic.documents.filter((document) => document.category === "troubleshooting").slice(0, 1),
  );
  const featuredTroubleshooting = [
    ...firstTroubleshootingByTopic,
    ...troubleshooting.filter(
      (document) => !firstTroubleshootingByTopic.some((featured) => featured.id === document.id),
    ),
  ].slice(0, 4);

  return (
    <>
      <main>
        <section className="hero">
          <div className="hero-grid" aria-hidden="true" />
          <div className="hero-copy">
            <span className="eyebrow"><i /> LEARN · OPERATE · SOLVE</span>
            <h1>운영 지식을<br /><em>내 것</em>으로 만드는 곳.</h1>
            <p>개념을 이해하고, 직접 확인하고, 문제를 해결하는 흐름으로<br className="desktop-break" /> DevOps를 차근차근 익혀보세요.</p>
            <div className="hero-actions">
              <button className="primary-button" onClick={() => document.querySelector("#tracks")?.scrollIntoView({ behavior: "smooth" })}>
                학습 시작하기 <ArrowIcon />
              </button>
              <span>총 <strong>{documents.length}</strong>개의 실전 노트</span>
            </div>
          </div>
          <div className="hero-visual" aria-hidden="true">
            <div className="orbit orbit-one" />
            <div className="orbit orbit-two" />
            <div className="hero-terminal">
              <div className="terminal-bar"><i /><i /><i /><span>devops-note</span></div>
              <div className="terminal-content">
                <p><b>$</b> docker ps</p>
                <p className="muted">CONTAINER&nbsp;&nbsp;STATUS&nbsp;&nbsp;PORTS</p>
                <p><span>web</span>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Up&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;0.0.0.0:8080</p>
                <p className="terminal-gap"><b>$</b> redis-cli ping</p>
                <p className="terminal-success">PONG <i /></p>
              </div>
            </div>
            <span className="floating-label label-one">01 / CONCEPT</span>
            <span className="floating-label label-two">02 / PRACTICE</span>
            <span className="floating-label label-three">03 / SOLVE</span>
          </div>
        </section>

        <section className="search-section" aria-label="문서 검색">
          <div className="search-box">
            <SearchIcon />
            <input
              aria-label="학습 노트 검색"
              onChange={(event) => setQuery(event.target.value)}
              placeholder="무엇을 찾고 있나요?  예: Docker 네트워크, Redis Sentinel"
              value={query}
            />
            <kbd>⌘ K</kbd>
          </div>
          <div className="topic-filter" aria-label="주제 필터">
            <button className={selectedTopic === "all" ? "active" : ""} onClick={() => setSelectedTopic("all")}>전체</button>
            {topics.map((topic) => <button className={selectedTopic === topic.id ? "active" : ""} key={topic.id} onClick={() => setSelectedTopic(topic.id)}>{topic.title}</button>)}
          </div>
          {normalizedQuery && (
            <div className="search-results">
              <div className="result-heading"><strong>검색 결과</strong><span>{searchResults.length}개 문서</span></div>
              {searchResults.length ? searchResults.map((document) => {
                const topic = topicMap.get(document.topicId)!;
                return (
                  <button className="search-result" key={document.id} onClick={() => onOpenDocument(document.id)}>
                    <TopicMark topic={topic} small />
                    <span><b>{document.title}</b><small>{topic.title} · {document.categoryLabel} · {document.readTime}분</small></span>
                    <ArrowIcon />
                  </button>
                );
              }) : <p className="empty-result">일치하는 노트가 없습니다. 더 짧은 검색어로 찾아보세요.</p>}
            </div>
          )}
        </section>

        <section className="tracks section-shell" id="tracks">
          <div className="section-heading">
            <div><span className="section-number">01</span><p className="eyebrow">LEARNING TRACKS</p><h2>주제별로 차근차근</h2></div>
            <p>기초 개념부터 운영 판단까지,<br />추천 순서대로 학습해보세요.</p>
          </div>
          <div className="topic-grid">
            {topics.map((topic) => {
              const trackDocuments = topic.documents.filter((document) => document.category === "concepts");
              const completedCount = topic.documents.filter((document) => completed.has(document.id)).length;
              const progress = topic.documents.length ? Math.round((completedCount / topic.documents.length) * 100) : 0;
              const style = { "--topic-accent": topic.accent, "--topic-soft": topic.accentSoft } as CSSProperties;
              return (
                <article className="topic-card" key={topic.id} style={style}>
                  <div className="topic-card-top"><TopicMark topic={topic} /><span>{topic.kicker}</span><small>{String(topic.order).padStart(2, "0")}</small></div>
                  <h3>{topic.title}</h3>
                  <p>{topic.description}</p>
                  <div className="tag-row">{topic.tags.map((tag) => <span key={tag}>#{tag}</span>)}</div>
                  <div className="track-list">
                    {trackDocuments.slice(0, 4).map((document, index) => (
                      <button key={document.id} onClick={() => onOpenDocument(document.id)}>
                        <span className={completed.has(document.id) ? "step complete" : "step"}>{completed.has(document.id) ? "✓" : index + 1}</span>
                        <span><b>{document.title}</b><small>{document.difficulty} · {document.readTime}분</small></span>
                        <ArrowIcon />
                      </button>
                    ))}
                  </div>
                  <div className="topic-card-footer">
                    <span><i style={{ width: `${progress}%` }} /></span>
                    <small>{completedCount}/{topic.documents.length} 완료</small>
                    <button onClick={() => onOpenDocument(trackDocuments[0]?.id ?? topic.documents[0]?.id)}>전체 보기 <ArrowIcon /></button>
                  </div>
                </article>
              );
            })}
            <article className="topic-card upcoming-card">
              <div className="upcoming-symbol"><span>+</span></div>
              <p className="eyebrow">NEXT TOPICS</p>
              <h3>다음 학습 주제도<br />계속 추가됩니다.</h3>
              <div className="upcoming-list"><span>Kubernetes</span><span>CI / CD</span><span>Monitoring</span><span>Linux</span></div>
              <p>새 디렉터리와 README를 추가하면<br />학습 카탈로그에 자동으로 연결됩니다.</p>
            </article>
          </div>
        </section>

        <section className="troubleshooting section-shell" id="troubleshooting">
          <div className="section-heading light-heading">
            <div><span className="section-number">02</span><p className="eyebrow">TROUBLESHOOTING</p><h2>문제에서 시작하는 학습</h2></div>
            <p>증상 → 원인 → 확인 → 해결 순서로<br />운영 중 마주친 문제를 빠르게 찾아보세요.</p>
          </div>
          <div className="trouble-grid">
            {featuredTroubleshooting.map((document, index) => (
              <button className="trouble-card" key={document.id} onClick={() => onOpenDocument(document.id)}>
                <span className="trouble-index">CASE {String(index + 1).padStart(2, "0")}</span>
                <span className="signal" aria-hidden="true"><i /><i /><i /></span>
                <h3>{document.title}</h3>
                <p>{document.summary}</p>
                <span className="trouble-link">해결 방법 보기 <ArrowIcon /></span>
              </button>
            ))}
          </div>
        </section>

        <section className="principle section-shell">
          <p className="eyebrow">HOW WE LEARN</p>
          <h2>짧게 읽고, 직접 확인하고,<br /><em>운영 관점</em>으로 판단합니다.</h2>
          <div className="principle-steps">
            <div><span>01</span><b>개념 이해</b><p>무엇이고 왜 필요한지<br />핵심부터 짚습니다.</p></div>
            <i />
            <div><span>02</span><b>직접 확인</b><p>명령어와 최소 예제로<br />동작을 검증합니다.</p></div>
            <i />
            <div><span>03</span><b>문제 해결</b><p>증상과 원인을 연결해<br />재현 가능한 답을 찾습니다.</p></div>
          </div>
        </section>
      </main>
      <footer><button className="brand footer-brand" onClick={() => window.scrollTo({ top: 0, behavior: "smooth" })}><span className="brand-symbol"><i /><i /><i /></span><span><b>DEVOPS</b><small>NOTE</small></span></button><p>운영 지식을 기록하고, 연결하고, 다시 꺼내 씁니다.</p><span>LEARN · OPERATE · SOLVE</span></footer>
    </>
  );
}

function DocumentView({
  document,
  completed,
  onBack,
  onOpenDocument,
  onToggleComplete,
}: {
  document: DocumentEntry;
  completed: Set<string>;
  onBack: () => void;
  onOpenDocument: (id: string) => void;
  onToggleComplete: (id: string) => void;
}) {
  const topic = topicMap.get(document.topicId)!;
  const topicDocuments = topic.documents;
  const currentIndex = topicDocuments.findIndex((entry) => entry.id === document.id);
  const previous = topicDocuments[currentIndex - 1];
  const next = topicDocuments[currentIndex + 1];
  const progress = Math.round((topicDocuments.filter((entry) => completed.has(entry.id)).length / topicDocuments.length) * 100);
  const style = { "--topic-accent": topic.accent, "--topic-soft": topic.accentSoft } as CSSProperties;

  return (
    <main className="reader" style={style}>
      <aside className="reader-sidebar">
        <button className="back-button" onClick={onBack}><ArrowIcon direction="left" /> 전체 학습으로</button>
        <div className="reader-topic"><TopicMark topic={topic} /><span><small>{topic.kicker}</small><b>{topic.title}</b></span></div>
        <div className="reader-progress"><span><b>{progress}%</b> 학습 완료</span><i><em style={{ width: `${progress}%` }} /></i></div>
        <nav aria-label={`${topic.title} 문서 목록`}>
          {["concepts", "commands", "examples", "troubleshooting"].map((category) => {
            const entries = topicDocuments.filter((entry) => entry.category === category);
            if (!entries.length) return null;
            return (
              <div className="reader-group" key={category}>
                <p>{entries[0].categoryLabel}</p>
                {entries.map((entry, index) => (
                  <button className={entry.id === document.id ? "active" : ""} key={entry.id} onClick={() => onOpenDocument(entry.id)}>
                    <span>{completed.has(entry.id) ? "✓" : String(index + 1).padStart(2, "0")}</span>{entry.title}
                  </button>
                ))}
              </div>
            );
          })}
        </nav>
      </aside>
      <section className="reader-main">
        <div className="reader-breadcrumb"><button onClick={onBack}>학습 홈</button><span>/</span><span>{topic.title}</span><span>/</span><strong>{document.categoryLabel}</strong></div>
        <article className="article-shell">
          <header className="article-header">
            <div className="article-meta"><span>{document.categoryLabel}</span><span>{document.difficulty}</span><span>약 {document.readTime}분</span></div>
            <h1>{document.title}</h1>
            <p>{document.summary}</p>
            <div className="article-actions">
              <div className="tag-row">{document.tags.map((tag) => <span key={tag}>#{tag}</span>)}</div>
              <button className={completed.has(document.id) ? "completion-button completed" : "completion-button"} onClick={() => onToggleComplete(document.id)}>
                {completed.has(document.id) ? "✓ 학습 완료" : "학습 완료로 표시"}
              </button>
            </div>
          </header>
          <MarkdownDocument document={document} onOpenDocument={onOpenDocument} />
          <nav className="article-pagination" aria-label="이전 및 다음 문서">
            {previous ? <button onClick={() => onOpenDocument(previous.id)}><ArrowIcon direction="left" /><span><small>이전 노트</small><b>{previous.title}</b></span></button> : <span />}
            {next ? <button className="next" onClick={() => onOpenDocument(next.id)}><span><small>다음 노트</small><b>{next.title}</b></span><ArrowIcon /></button> : <span />}
          </nav>
        </article>
      </section>
      <aside className="toc">
        <p>ON THIS PAGE</p>
        {document.sections.map((section) => <a href={`#${slugify(section)}`} key={section}>{section}</a>)}
        <div><span>문서 경로</span><code>{document.sourcePath}</code></div>
      </aside>
    </main>
  );
}

export default function Home() {
  const [activeDocumentId, setActiveDocumentId] = useState<string | null>(null);
  const { completed, toggle } = useCompletion();

  useEffect(() => {
    const updateFromUrl = () => {
      const id = new URLSearchParams(window.location.search).get("doc");
      setActiveDocumentId(id && documentMap.has(id) ? id : null);
    };
    updateFromUrl();
    window.addEventListener("popstate", updateFromUrl);
    return () => window.removeEventListener("popstate", updateFromUrl);
  }, []);

  function openDocument(id: string) {
    if (!documentMap.has(id)) return;
    window.history.pushState({}, "", `?doc=${encodeURIComponent(id)}`);
    setActiveDocumentId(id);
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  function goHome() {
    window.history.pushState({}, "", window.location.pathname);
    setActiveDocumentId(null);
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  const activeDocument = activeDocumentId ? documentMap.get(activeDocumentId) : null;
  return (
    <div className="site-frame">
      <Header onHome={goHome} />
      {activeDocument ? (
        <DocumentView document={activeDocument} completed={completed} onBack={goHome} onOpenDocument={openDocument} onToggleComplete={toggle} />
      ) : (
        <HomeView completed={completed} onOpenDocument={openDocument} />
      )}
    </div>
  );
}
