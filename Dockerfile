FROM golang:1.25-alpine as build

RUN apk add --no-cache \
    git \
    gcc \
    musl-dev \
    pkgconfig

WORKDIR /app

COPY go.mod go.sum /app/

ARG GITHUB_USER
ARG GITHUB_PAT

RUN if [ -n "$GITHUB_USER" ] && [ -n "$GITHUB_PAT" ]; then \
        git config --global url."https://${GITHUB_USER}:${GITHUB_PAT}@github.com/".insteadOf "https://github.com/"; \
    fi

RUN go mod download && go mod verify

COPY ./src /app/src
COPY ./cmd /app/cmd
COPY ./main.go /app/

RUN if [ -n "$GITHUB_USER" ] && [ -n "$GITHUB_PAT" ]; then \
        git config --global --unset url."https://${GITHUB_USER}:${GITHUB_PAT}@github.com/".insteadOf; \
    fi

RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
    -tags musl \
    -ldflags="-w -s" \
    -trimpath \
    -o ./bin/pgbridge ./main.go

FROM alpine:3.19



WORKDIR /app

COPY --from=build /app/bin/pgbridge /app/pgbridge

EXPOSE 9090

ENTRYPOINT ["/app/pgbridge"]
