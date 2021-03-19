FROM node:alpine as builder
WORKDIR /usr/src/app
COPY ./package.json .
RUN npm install
ARG REACT_APP_SERVER_ADDRESS
COPY . .
RUN npm run build

FROM nginx:mainline
EXPOSE 3000
COPY ./nginx/default.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /usr/src/app/build /usr/share/nginx/html