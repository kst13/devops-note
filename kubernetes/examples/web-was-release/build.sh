#!/usr/bin/env bash
# Jenkins Job 두 개(web, was)가 할 일을 로컬에서 대신한다. 앱마다 "이미지 build·push → 같은 버전으로 차트 package(·업로드)".
#   ./build.sh 1.0.0            web, was 둘 다
#   ./build.sh 1.0.1 was        was 만 (Jenkins 에서 was Job 만 돈 상황)
#   CHART_REPO=http://192.168.64.2:8085 ./build.sh 1.0.2    차트를 ChartMuseum 에 업로드까지
# 환경변수:
#   REGISTRY   이미지 레지스트리 (기본 192.168.64.3:5050 = dev1 의 registry Pod)
#   CHART_REPO ChartMuseum 주소. 비우면 패키지만 만들고 업로드는 건너뜀
set -euo pipefail

VER="${1:?usage: build.sh <version> [web|was]   (예: 1.0.0)}"
APPS="${2:-web was}"
REGISTRY="${REGISTRY:-192.168.64.3:5050}"
CHART_REPO="${CHART_REPO:-}"
cd "$(dirname "$0")"
mkdir -p dist

for APP in $APPS; do
  case "$APP" in
    web) IMAGE="web-portal" ;;
    was) IMAGE="was-order"
         echo "== [$APP] jar 빌드 (Gradle)"
         ( cd was && ./gradlew -q bootJar -PappVersion="$VER" ) ;;
    *)   echo "알 수 없는 앱: $APP (web|was)"; exit 1 ;;
  esac

  echo "== [$APP] 이미지 build·push → $REGISTRY/$IMAGE:$VER"
  docker build -q --build-arg APP_VERSION="$VER" -t "$REGISTRY/$IMAGE:$VER" "$APP"
  docker push -q "$REGISTRY/$IMAGE:$VER"

  echo "== [$APP] 차트 package (version = appVersion = $VER)"
  helm lint "$APP/chart" >/dev/null
  helm package "$APP/chart" --version "$VER" --app-version "$VER" -d dist >/dev/null
  echo "   dist/$IMAGE-$VER.tgz"

  if [[ -n "$CHART_REPO" ]]; then
    echo "== [$APP] 차트 업로드 → $CHART_REPO"
    # ChartMuseum: POST /api/charts.  Nexus helm hosted: curl -u user:pw --upload-file dist/$IMAGE-$VER.tgz $NEXUS/repository/helm-hosted/
    curl -fsS --data-binary "@dist/$IMAGE-$VER.tgz" "$CHART_REPO/api/charts" && echo
  fi
done

echo "완료: $VER ($APPS)"
echo "태그 확인: curl -s http://$REGISTRY/v2/was-order/tags/list"
