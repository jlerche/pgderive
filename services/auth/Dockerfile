FROM node:24-bookworm-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY auth.mjs migrate.mjs server.mjs ./
USER node
CMD ["npm", "start"]
