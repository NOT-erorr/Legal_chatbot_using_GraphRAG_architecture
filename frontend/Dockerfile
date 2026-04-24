# Stage 1: Build React SPA
FROM node:22-alpine AS builder

WORKDIR /app
COPY package*.json ./
RUN npm install

COPY . .
# We use Vite, so 'npm run build' will bundle output to '/app/dist'
ARG VITE_API_URL
ENV VITE_API_URL=$VITE_API_URL
RUN npm run build

# Stage 2: Serve using Nginx (Acting as Reverse Proxy / API Gateway)
FROM nginx:alpine

COPY --from=builder /app/dist /usr/share/nginx/html
# Replace default config with our custom proxy+routing config
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]
