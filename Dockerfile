FROM node:22-bookworm-slim AS builder

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY tsconfig.json ./
COPY src/ ./src/

RUN npm run build

FROM node:22-bookworm-slim

ENV NODE_ENV=production
ENV PORT=10000

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/dist ./dist
COPY package.json ./
COPY server/ ./server/
COPY mcp/ ./mcp/
COPY sdk/ ./sdk/

USER node

EXPOSE 10000

CMD ["node", "server/index.mjs"]
