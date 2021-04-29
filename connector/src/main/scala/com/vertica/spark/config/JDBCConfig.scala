// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.vertica.spark.config

/**
 * Represents any config necessary for authenticating to JDBC.
 *
 * Abstract as there are multiple possible methods of authentication.
 */
trait JdbcAuth {
  def user: String
}

/**
 * Authentication to Vertica using username and password
 */
case class BasicJdbcAuth(username: String, password: String) extends JdbcAuth {
  override def user: String = username
}

/**
 * Authentication using kerberos
 * @param kerberosServiceName the Kerberos service name, as specified when creating the service principal
 * @param kerberosHostname the Kerberos host name, as specified when creating the service principal
 * @param jaasConfigName the name of the JAAS configuration used for Kerberos authentication
 */
case class KerberosAuth(username: String,
                        kerberosServiceName: String,
                        kerberosHostname: String,
                        jaasConfigName: String) extends JdbcAuth {
  override def user: String = username
}

/**
 * Configuration for a JDBC connection to Vertica.
 *
 * @param host hostname for the JDBC connection
 * @param port port for the JDBC connection
 * @param db name of the Vertica database to connect to
 * @param auth the authentication details, varies depending on method used
 * @param sslConfig the ssl configuration settings for the JDBC connection
 */
final case class JDBCConfig(host: String,
                            port: Int,
                            db: String,
                            auth: JdbcAuth,
                            sslConfig: JDBCSSLConfig)

/**
 * SSL configuration settings for a JDBC connection to Vertica.
 *
 * @param ssl flag indicating whether to enable SSL for the connection or not
 * @param keyStorePath path to the key store
 * @param keyStorePassword password for the key store
 * @param trustStorePath path to the trust store
 * @param trustStorePassword password for the trust store
 */
case class JDBCSSLConfig(ssl: Boolean,
                         keyStorePath: Option[String],
                         keyStorePassword: Option[String],
                         trustStorePath: Option[String],
                         trustStorePassword: Option[String])
