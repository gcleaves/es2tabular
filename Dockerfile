# Build stage
FROM node:20-slim AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm ci --only=production

# Production stage
FROM node:20-slim

WORKDIR /app

# Install dumb-init and gosu for proper signal handling and user switching
RUN apt-get update && apt-get install -y --no-install-recommends \
    dumb-init \
    gosu \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user with a proper home directory
RUN groupadd -r nodejs && useradd -r -g nodejs -m -d /home/nodejs nodejs

# Copy node_modules from builder
COPY --from=builder /app/node_modules ./node_modules

# Copy application files
COPY package*.json ./
COPY index.js ./
COPY server.js ./
COPY lib/ ./lib/
COPY public/ ./public/

# Create data directory
RUN mkdir -p /app/data

# Copy and setup entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Expose port
EXPOSE 3000

# Set environment variables
ENV NODE_ENV=production
ENV PORT=3000
ENV HOME=/home/nodejs

# Use dumb-init as entrypoint for proper signal handling
ENTRYPOINT ["dumb-init", "--", "docker-entrypoint.sh"]

# Start the application
CMD ["node", "server.js"]
