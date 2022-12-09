# Set Up
```
# start server
cd ~/FiGHtDDB
sh util/deploy.sh

# import benchmark data into database
sh util/deploy.sh
```

# Usage
```
# run hard cord query
cd ~/FiGHtDDB
go run client/client.go

# then enter 1-10 to execute corresponding query
# enter 'q' to exit
```
# Develop
```
modify SendSql in server/server.go

# modify grpc service
cd comm
protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    comm.proto
```