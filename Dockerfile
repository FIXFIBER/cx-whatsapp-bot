# Render backend Dockerfile
FROM node:20-alpine

WORKDIR /app

# Install deps first (better layer caching)
COPY package.json ./
RUN npm ci --omit=dev || npm install --omit=dev

# App source
COPY . .

# Render mounts a persistent disk at /data (DATA_DIR) for the WhatsApp session.
ENV DATA_DIR=/data
ENV PORT=3001
EXPOSE 3001

CMD ["npm", "start"]
