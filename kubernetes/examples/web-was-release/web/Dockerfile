FROM nginx:1.28-alpine

ARG APP_VERSION=dev
# WAS Service 의 FQDN. 네임스페이스가 다르면 Deployment 의 env 로 덮어쓴다.
ENV WAS_HOST=was-order.webwas.svc.cluster.local

COPY default.conf.template /etc/nginx/templates/default.conf.template
COPY --chmod=755 05-dns.envsh /docker-entrypoint.d/05-dns.envsh
COPY index.html /usr/share/nginx/html/index.html
# 화면에 web 버전을 박아 두어 web 이미지가 바뀐 것도 눈에 보이게 한다.
RUN sed -i "s/__WEB_VERSION__/${APP_VERSION}/" /usr/share/nginx/html/index.html
