echo ========= Build docker image
docker build -t otus.lessons.22.01 .
# echo ========= Execute bulk_server
# docker run --rm -i otus.lessons.22.01 
# ( ./bulk_server 9999 3 ) & (sleep 1 ; seq 1 10 | nc localhost 9999 ; pkill -INT bulk_server)
echo ========= Remove docker image
docker rmi otus.lessons.22.01