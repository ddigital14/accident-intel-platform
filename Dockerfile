FROM node:20-alpine

WORKDIR /app

# Install dependencies
COPY package*.json ./
RUN npm ci --production

# Copy source
COPY src/ ./src/
COPY frontend/build/ ./frontend/build/

# Create logs directory
RUN mkdir -p logs

ENV NODE_ENV=production
ENV PORT=3001

EXPOSE 3001

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
  CMD wget -qO- http://localhost:3001/health || exit 1

CMD ["node", "src/server.js"]
