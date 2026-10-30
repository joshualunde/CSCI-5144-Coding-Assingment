# ---- Build stage: compiler lives here only ----
FROM alpine:3.20 AS build
RUN apk add --no-cache build-base
WORKDIR /src
COPY server.c kv_store.c kv_store.h ./
RUN gcc -O2 -Wall -Wextra -pthread -o kv_server server.c kv_store.c

# ---- Runtime stage: small final image ----
FROM alpine:3.20
RUN adduser -D -H kv
WORKDIR /data
RUN chown kv /data
COPY --from=build /src/kv_server /usr/local/bin/kv_server

ENV KV_BIND=0.0.0.0
USER kv
VOLUME /data
EXPOSE 8080
ENTRYPOINT ["kv_server"]
CMD ["8080"]
