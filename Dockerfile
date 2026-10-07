FROM node:24-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

FROM node:24-alpine AS runtime
ENV NODE_ENV=production
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY package.json bridge.js config.js flatten.js database.js processor.js errors.js ./
RUN addgroup -S app && adduser -S -G app app
USER app
CMD ["node", "bridge.js"]
