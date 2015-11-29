package cassandra

import com.datastax.driver.core.{Session, Cluster}

/**
 *
 */
class CassandraConnector(val session : Session) {

  val cluster = session.getCluster

  def close = cluster.close

  val metadata = cluster.getMetadata

}

object CassandraConnector {

  def connect(node: String, port: Int): CassandraConnector = {
    val cluster = Cluster.builder().addContactPoint(node).withPort(port).build()
    new CassandraConnector(cluster.connect())
  }

}
