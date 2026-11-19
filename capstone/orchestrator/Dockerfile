
FROM node:20-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY shared ./shared
COPY orchestrator ./orchestrator

EXPOSE 8802

CMD ["npx", "tsx", "orchestrator/src/index.ts"]
