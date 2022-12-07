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
```