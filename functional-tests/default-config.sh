echo -e 'functional-tests {
  host="'"vertica"'"
  port=5433
  db="'"docker"'"
  user="'"dbadmin"'"
  password="'""'"
  filepath="'"webhdfs://hdfs:50070/data/"'"
  tlsmode="disable"
  truststorepath="'"/truststore.jks"'"
  truststorepassword="'"dbadmin"'"
  }' > ./src/main/resources/application.conf