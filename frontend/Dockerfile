FROM node:22-slim AS dev

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm install

COPY . .
RUN chown -R node:node /app
USER node

EXPOSE 3000
CMD ["npm", "run", "dev"]
