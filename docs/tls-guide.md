# Configuring TLS with the Connector

In order to use TLS with the connector, you will need to setup Vertica as a TLS server, and the host containing the application that uses the connector as a TLS client.

The following two sections are meant to be followed in order, with the client configuration following the Vertica configuration. Please note that this guide only uses a self-signed certificate.

## Setting up Vertica as a TLS server

Simply follow the instructions [here](https://www.vertica.com/kb/Using-SSL-Server-Authentication-with-Vertica-Validating-Your-SSL/Content/BestPractices/Using-SSL-Server-Authentication-with-Vertica-Validating-Your-SSL.htm).

## Setting up the client machine as a TLS client

Copy the server.crt certificate created on the Vertica server to the client machine.

Run the following command on the client machine:
`keytool -keystore truststore.jks -alias bmc -import -file server.crt`

Note: `keytool` is included as part of the Java runtime. If you do not have it, then you may need to install Java first.

This will create the truststore file on the client side, prompt you to create a new password for it, and import the server.crt self-signed certificate into the truststore.

Set the `tls_mode`, `trust_store_path`, and `trust_store_password` properties in the connector options:
```
"tls_mode" -> "disable"
"trust_store_path" -> "/truststore.jks"
"trust_store_password" -> "testpass"
```

Here, the absolute path `/truststore.jks` was used. Set this path to wherever you created your truststore.jks file. You will also need to set `trust_store_password` to the password you set on your truststore.jks file.
