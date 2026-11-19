
FROM node:20-bookworm-slim

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig.json ./
COPY shared ./shared
COPY observer ./observer

EXPOSE 8801

CMD ["npx", "tsx", "observer/src/index.ts"]
