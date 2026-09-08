FROM mcr.microsoft.com/playwright:v1.61.1-noble@sha256:5b8f294aff9041b7191c34a4bab3ac270157a28774d4b0660e9743297b697e48

WORKDIR /opt/chitragupta-demo-media

ENV HOME=/tmp/chitragupta-demo-media-home
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

RUN mkdir -p "$HOME" && chmod 1777 "$HOME"

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts

COPY capture.mjs /opt/chitragupta-demo-media/capture.mjs

ENTRYPOINT ["node", "/opt/chitragupta-demo-media/capture.mjs"]
