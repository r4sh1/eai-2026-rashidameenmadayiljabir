
FROM node:20-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY shared ./shared
COPY notifier ./notifier

EXPOSE 8803

CMD ["npx", "tsx", "notifier/src/index.ts"]
