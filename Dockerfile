FROM node:slim

WORKDIR /app

COPY package.json /app/package.json
RUN npm install
RUN mv /app/node_modules /node_modules

COPY . /app

LABEL authors="Dale Ewald <dale.ewald@gmail.com>" \
        version="1.0"

CMD ["npm", "start"]