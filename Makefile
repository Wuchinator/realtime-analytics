.PHONY: proto clean docker-up docker-down help

PROTO_DIR = api/proto
PROTO_OUT_DIR = pkg/pb

proto:
	@mkdir -p $(PROTO_OUT_DIR)/events
	@mkdir -p $(PROTO_OUT_DIR)/analytics
	
	protoc --go_out=$(PROTO_OUT_DIR)/events --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT_DIR)/events --go-grpc_opt=paths=source_relative \
		-I=$(PROTO_DIR) $(PROTO_DIR)/events.proto
	
	protoc --go_out=$(PROTO_OUT_DIR)/analytics --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT_DIR)/analytics --go-grpc_opt=paths=source_relative \
		-I=$(PROTO_DIR) $(PROTO_DIR)/analytics.proto
	

docker-up:
	docker-compose up -d
	@sleep 10
	@docker-compose ps

docker-down:
	docker-compose down

docker-clean:
	docker-compose down -v

run-event-service:
	go run cmd/event-service/main.go

run-analytics-service:
	go run cmd/analytics-service/main.go

run-query-service:
	go run cmd/query-service/main.go


run-all:
	@make docker-up
	@sleep 5
	@go run cmd/event-service/main.go > logs/event-service.log 2>&1 &
	@go run cmd/analytics-service/main.go > logs/analytics-service.log 2>&1 &
	@go run cmd/query-service/main.go > logs/query-service.log 2>&1 &


lint:
	golangci-lint run


clean:
	rm -rf $(PROTO_OUT_DIR)
	go clean -cache -testcache
