HOST1=10.77.50.211
HOST2=10.77.50.208
HOST3=10.77.50.209

NODE1=main
NODE2=segment1
NODE3=segment2
NODE4=segment3

DATADIR="/home/ddb/FiGHtDDB"

function sync {
    rm -rf deploy.log
    scp -r ${DATADIR} ddb@${HOST2}:~/ >> deploy.log 2>&1
    scp -r ${DATADIR} ddb@${HOST3}:~/ >> deploy.log 2>&1
}

function setupServer {
    ssh ddb@${HOST1} "screen -S ${NODE1} -X quit"
    ssh ddb@${HOST1} "cd ${DATADIR}; screen -S ${NODE1} -d -m; screen -r \"${NODE1}\" -X stuff $'go run server/server.go ${NODE1}\n'"
    ssh ddb@${HOST2} "screen -S ${NODE2} -X quit"
    ssh ddb@${HOST2} "cd ${DATADIR}; screen -S ${NODE2} -d -m; screen -r \"${NODE2}\" -X stuff $'go run server/server.go ${NODE2}\n'"
    ssh ddb@${HOST3} "screen -S ${NODE3} -X quit"
    ssh ddb@${HOST3} "cd ${DATADIR}; screen -S ${NODE3} -d -m; screen -r \"${NODE3}\" -X stuff $'go run server/server.go ${NODE3}\n'"
    ssh ddb@${HOST3} "screen -S ${NODE4} -X quit"
    ssh ddb@${HOST3} "cd ${DATADIR}; screen -S ${NODE4} -d -m; screen -r \"${NODE4}\" -X stuff $'go run server/server.go ${NODE4}\n'"
}

sync
setupServer