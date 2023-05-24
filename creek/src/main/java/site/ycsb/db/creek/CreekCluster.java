package site.ycsb.db.creek;

import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class CreekCluster {

  private BlockingQueue<Node> nodes;

  public CreekCluster(List<String> hosts, Properties properties) throws SQLException, InterruptedException {
    nodes = new LinkedBlockingDeque(hosts.size());
    for (String host : hosts) {
      nodes.put(new Node(host, properties));
    }
  }

  public Node getAvailableNode() throws InterruptedException {
    return nodes.take();
  }

  public void returnNode(Node node) throws InterruptedException {
    nodes.put(node);
  }

  public void closeConnections() throws SQLException{
    for (Node node : nodes) {
      node.closeConnection();
    }
  }

  public void removeDrivers() {
    for (Node node : nodes) {
      node.nullDriver();
    }
  }


  public static class Node {
    private final Connection connection;
    private Driver driver;

    public Node(String host, Properties properties) throws SQLException {
      this.driver = new Driver();
      properties.setProperty("options", "-c statement_timeout=90000");
      this.connection = driver.connect(host, properties);
    }

    public Connection connection() {
      return connection;
    }

    public void closeConnection() throws SQLException {
      connection.close();
    }

    public void nullDriver() {
      driver = null;
    }
  }

}
