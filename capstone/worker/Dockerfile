FROM node:20-bookworm-slim

WORKDIR /app

COPY worker/package.json worker/package-lock.json ./worker/

RUN cd worker && npm ci

RUN ln -s /app/worker/node_modules /app/node_modules

COPY worker/tsconfig.json ./worker/tsconfig.json

COPY worker/src ./worker/src

COPY shared/trace.ts ./shared/trace.ts
COPY shared/rabbit.ts ./shared/rabbit.ts

CMD ["./worker/node_modules/.bin/tsx", "worker/src/worker.ts"]