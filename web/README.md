# DevOps Note Web

저장소의 Docker, Redis 등 Markdown 문서를 읽기 쉬운 학습 경로와 문서 화면으로 제공하는 웹 프로젝트입니다.

## 로컬 실행

Node.js 22 이상이 필요합니다.

```bash
npm install
npm run dev
```

브라우저에서 `http://localhost:3000`을 엽니다.

## 콘텐츠 동기화

```bash
npm run sync-content
```

동기화 스크립트는 저장소 루트에서 `README.md`가 있는 주제 디렉터리를 찾고, 그 아래의 Markdown 파일을 `app/data/content.generated.json`으로 변환합니다. `npm run dev`와 `npm run build` 실행 시에도 자동으로 동기화됩니다. 생성된 JSON은 손으로 편집하지 않습니다.

주제별 표시 이름, 설명, 색상, 순서는 `content.config.json`에서 선택적으로 지정합니다. 설정이 없는 새 주제도 기본 스타일로 표시됩니다.

## 콘텐츠 작성 규칙

문서의 메타데이터는 별도 설정이 아니라 **파일 경로와 이름에서 자동으로 파생**됩니다. 파일을 옮기거나 이름을 바꾸면 화면의 정렬·배지·라벨이 조용히 바뀌므로 아래 규칙을 알고 작성해야 합니다.

- **카테고리** — 주제 아래 첫 번째 폴더명이 결정합니다: `concepts/`(개념), `troubleshooting/`(트러블슈팅), `commands/`(명령어), `examples/`(예제).
- **정렬 순서** — 카테고리 순서 × 1000 + 파일명의 두 자리 숫자 접두사(예: `03-persistence.md`). 접두사가 없으면 해당 카테고리의 맨 뒤로 갑니다.
- **난이도 배지** — 자동 추론됩니다: `troubleshooting` → 실전, `commands` → 참고, 숫자 접두사 5 이상 → 중급, 그 외 → 입문.
- **제목/요약** — 제목은 첫 `# H1`, 카드 요약은 H1 아래 첫 번째 "실질적인 일반 문단"입니다. H1 바로 아래에 리스트나 표가 아닌 문단을 반드시 둡니다.
- **목차** — 문서 내 목차는 `## H2`마다 생성됩니다.

렌더러는 라이브러리가 아니라 `app/page.tsx`에 직접 구현된 것으로, **Markdown의 서브셋만** 지원합니다: `##`/`###` 헤딩(`#`은 페이지가 제목을 따로 그리므로 본문에서 제거됨), 언어 태그 있는 펜스 코드 블록, `-`/`*`·순서 리스트(**중첩 불가**), `>` 인용, 파이프 표, 문단, 인라인 `` `code` ``·`**bold**`·`[텍스트](링크)`·bare URL. 이 밖의 문법(이미지, 각주, HTML 등)은 의도대로 렌더링되지 않으므로 새 문법은 브라우저에서 확인합니다.

문서 간 상대 링크는 실제 파일의 상대 경로로 적으면 앱 안에서 해당 문서를 여는 내비게이션 버튼으로 변환됩니다.

## 아키텍처 개요

```text
루트 Markdown 문서
  → scripts/sync-content.mjs        (스캔·메타데이터 파생)
  → app/data/content.generated.json (생성 파일, 손편집 금지)
  → app/page.tsx                    (전체 UI + 자체 Markdown 렌더러)
  → Cloudflare Workers (vinext)     (worker/index.ts 가 엔트리)
```

- Next.js 16 + React 19 + Vite + Tailwind CSS 4 구성이며, `vinext`로 Cloudflare Workers에 배포합니다.
- `npm test`는 먼저 빌드한 뒤 `tests/rendered-html.test.mjs`를 실행합니다. 이 테스트는 빌드 산출물(`dist/server/index.js`)을 임포트해 렌더링 결과의 리터럴 문자열과 주제 id 목록을 단언하므로, 소스 수정은 빌드 후에만 반영되고 **주제 추가·삭제 시 테스트의 주제 목록도 함께 갱신**해야 합니다.

## 주요 명령어

```bash
npm run build   # 배포용 빌드 생성
npm test        # 빌드 후 렌더링 테스트
npm run lint    # TypeScript/React 정적 검사
```
