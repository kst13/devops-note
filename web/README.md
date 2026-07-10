# DevOps Note Web

저장소의 Docker, Redis 등 Markdown 문서를 읽기 쉬운 학습 경로와 문서 화면으로 제공하는 웹 프로젝트입니다.

## 로컬 실행

```bash
npm install
npm run dev
```

브라우저에서 `http://localhost:3000`을 엽니다.

## 콘텐츠 동기화

```bash
npm run sync-content
```

동기화 스크립트는 저장소 루트에서 `README.md`가 있는 주제 디렉터리를 찾고, 그 아래의 Markdown 파일을 `app/data/content.generated.json`으로 변환합니다. `npm run dev`와 `npm run build` 실행 시에도 자동으로 동기화됩니다.

주제별 표시 이름, 설명, 색상, 순서는 `content.config.json`에서 선택적으로 지정합니다. 설정이 없는 새 주제도 기본 스타일로 표시됩니다.

## 주요 명령어

```bash
npm run build   # 배포용 빌드 생성
npm test        # 빌드 후 렌더링 테스트
npm run lint    # TypeScript/React 정적 검사
```
