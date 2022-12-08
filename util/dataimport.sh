HOST1=10.77.50.211
HOST2=10.77.50.208
HOST3=10.77.50.209

NODE1=main
NODE2=segment1
NODE3=segment2
NODE4=segment3

PG1=postgres-hsy
PG2=postgres-hsy
PG3=postgres-hsy1
PG4=postgres-hsy2

PUBLISHER="drop table if exists publisher; create table publisher (id int primary key, name char(100), nation char(3));"
CUSTOMER="drop table if exists customer; create table customer (id int primary key, name char (25), rank int);"
BOOK="drop table if exists book; create table book (id int primary key, name char(100), authors char(200), publisher_id int, copies int);"
ORDERS="drop table if exists orders; create table orders (customer_id int, book_id int, quantity int);"

DATASOURCE="~/FiGHtDDB/data"
DATADEST="/home/"
DATAPATH="/home/data/"

COPYPUBLISHER="copy publisher from '${DATAPATH}publisher.tsv';"
COPYCUSTOMER="copy customer from '${DATAPATH}customer.tsv';"
COPYBOOK="copy book from '${DATAPATH}book.tsv';"
COPYORDERS="copy orders from '${DATAPATH}orders.tsv';"

DELETEPUBLISHER1="delete from publisher where not (id < 104000 and nation = 'PRC');"
DELETEPUBLISHER2="delete from publisher where not (id < 104000 and nation = 'USA');"
DELETEPUBLISHER3="delete from publisher where not (id >= 104000 and nation = 'PRC');"
DELETEPUBLISHER4="delete from publisher where not (id >= 104000 and nation = 'USA');"
DELETECUSTOMER1="alter table customer drop column rank;"
DELETECUSTOMER2="alter table customer drop column name;"
DELETEBOOK1="delete from book where not (id < 205000);"
DELETEBOOK2="delete from book where not (id >= 205000 and id < 210000);"
DELETEBOOK3="delete from book where not (id >= 210000);"
DELETEORDERS1="delete from orders where not (customer_id < 307000 and book_id < 215000);"
DELETEORDERS2="delete from orders where not (customer_id < 307000 and book_id >= 215000);"
DELETEORDERS3="delete from orders where not (customer_id >= 307000 and book_id < 215000);"
DELETEORDERS4="delete from orders where not (customer_id >= 307000 and book_id >= 215000);"

function createTable {
    ssh ddb@${HOST1} "docker exec ${PG1} psql -U postgres -c \"${PUBLISHER}${CUSTOMER}${BOOK}${ORDERS}\""
    ssh ddb@${HOST2} "docker exec ${PG2} psql -U postgres -c \"${PUBLISHER}${CUSTOMER}${BOOK}${ORDERS}\""
    ssh ddb@${HOST3} "docker exec ${PG3} psql -U postgres -c \"${PUBLISHER}${CUSTOMER}${BOOK}${ORDERS}\""
    ssh ddb@${HOST3} "docker exec ${PG4} psql -U postgres -c \"${PUBLISHER}${CUSTOMER}${BOOK}${ORDERS}\""
}

function mvfile {
    ssh ddb@${HOST1} "docker cp ${DATASOURCE} ${PG1}:${DATADEST}"
    ssh ddb@${HOST2} "docker cp ${DATASOURCE} ${PG2}:${DATADEST}"
    ssh ddb@${HOST3} "docker cp ${DATASOURCE} ${PG3}:${DATADEST}"
    ssh ddb@${HOST3} "docker cp ${DATASOURCE} ${PG4}:${DATADEST}"
}

function importDatabase {
    ssh ddb@${HOST1} "docker exec ${PG1} psql -U postgres -c \"${COPYPUBLISHER}${COPYCUSTOMER}${COPYBOOK}${COPYORDERS}
    ${DELETEPUBLISHER1}${DELETECUSTOMER1}${DELETEBOOK1}${DELETEORDERS1}\""
    ssh ddb@${HOST2} "docker exec ${PG2} psql -U postgres -c \"${COPYPUBLISHER}${COPYCUSTOMER}${COPYBOOK}${COPYORDERS}
    ${DELETEPUBLISHER2}${DELETECUSTOMER2}${DELETEBOOK2}${DELETEORDERS2}\""
    ssh ddb@${HOST3} "docker exec ${PG3} psql -U postgres -c \"${COPYPUBLISHER}${COPYCUSTOMER}${COPYBOOK}${COPYORDERS}
    ${DELETEPUBLISHER3}${DELETECUSTOMER3}${DELETEBOOK3}${DELETEORDERS3}\""
    ssh ddb@${HOST3} "docker exec ${PG4} psql -U postgres -c \"${COPYPUBLISHER}${COPYCUSTOMER}${COPYBOOK}${COPYORDERS}
    ${DELETEPUBLISHER4}${DELETECUSTOMER4}${DELETEBOOK4}${DELETEORDERS4}\""
}

createTable
mvfile
importDatabase