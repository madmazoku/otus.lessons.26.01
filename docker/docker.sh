echo ========= Build docker image
docker build -t otus.lessons.26.01 .
# echo ========= Execute bulk_server
# docker run --rm -i otus.lessons.26.01 
# ( ./join_server 9999 ) & (perl -e 'print "INSERT A 1 A\nINSERT B 1 B\nDUMP A\nDUMP B\nINTERSECTION\n";' | nc localhost 9999 ; pkill -INT bulk_server)
echo ========= Remove docker image
docker rmi otus.lessons.26.01