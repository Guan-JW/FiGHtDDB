PROJECTDIR="/home/ddb/FiGHtDDB"

HOST1=10.77.50.211
HOST2=10.77.50.208
HOST3=10.77.50.209

PG1=postgres-hsy
PG2=postgres-hsy
PG3=postgres-hsy1
PG4=postgres-hsy2

DROPPUBLISHER="drop table if exists publisher;"
DROPCUSTOMER="drop table if exists customer;"
DROPBOOK="drop table if exists book;"
DROPORDERS="drop table if exists orders;"

function dropTable {
    ssh ddb@${HOST1} "docker exec ${PG1} psql -U postgres -c \"${DROPPUBLISHER}${DROPCUSTOMER}${DROPBOOK}${DROPORDERS}\""
    ssh ddb@${HOST2} "docker exec ${PG2} psql -U postgres -c \"${DROPPUBLISHER}${DROPCUSTOMER}${DROPBOOK}${DROPORDERS}\""
    ssh ddb@${HOST3} "docker exec ${PG3} psql -U postgres -c \"${DROPPUBLISHER}${DROPCUSTOMER}${DROPBOOK}${DROPORDERS}\""
    ssh ddb@${HOST3} "docker exec ${PG4} psql -U postgres -c \"${DROPPUBLISHER}${DROPCUSTOMER}${DROPBOOK}${DROPORDERS}\""
}

function dropMeta {
    cd ${PROJECTDIR}
    go run main.go
}

dropTable
dropMeta