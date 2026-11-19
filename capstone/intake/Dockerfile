
FROM node:20-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY shared ./shared
COPY intake ./intake

EXPOSE 8800

CMD ["npx", "tsx", "intake/src/index.ts"]
