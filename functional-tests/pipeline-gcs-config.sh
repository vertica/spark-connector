echo -e 'functional-tests={
  host="'"vertica"'"
  port="'"5433"'"
  db="'"docker"'"
  user="'"dbadmin"'"
  password="'""'"
  log='true'
  filepath="'"$GCS_FILEPATH"'"
  tlsmode="'"disable"'"
  truststorepath="'"/truststore.jks"'"
  truststorepassword="'"dbadmin"'"
}' > ./src/main/resources/application.conf