FROM nginx:1.28-alpine

ARG APP_VERSION=dev
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY index.html /usr/share/nginx/html/index.html
# 화면에 web 버전을 박아 두어 web 이미지가 바뀐 것도 눈에 보이게 한다.
RUN sed -i "s/__WEB_VERSION__/${APP_VERSION}/" /usr/share/nginx/html/index.html
