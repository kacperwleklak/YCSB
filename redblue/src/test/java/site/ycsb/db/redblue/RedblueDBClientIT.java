package site.ycsb.db.redblue;


import org.postgresql.Driver;

import java.sql.*;
import java.util.Properties;

class RedblueDBClientIT {

  public static void main(String[] args) throws SQLException {
    Properties tmpProps = new Properties();
    tmpProps.setProperty("user", "user");
    tmpProps.setProperty("password", "passwd");
    Driver driver = new Driver();
    Connection connection = driver.connect("jdbc:postgresql://localhost:5433/postgres", tmpProps);

//    CallableStatement callableStatement = connection.prepareCall("{CALL REVERSE_CASE(?, ?)}");
//    callableStatement.setString(1, "34ea1789-85ca-406a-9aff-76b2a428a792");
//    callableStatement.setFloat(2, 1);
//    callableStatement.execute();

//    String sql_update = "call REVERSE_CASE('siema', 1);";
//    Statement st_2 = connection.createStatement();
//    boolean execute = st_2.execute(sql_update);

    String sql_update2 = "call REVERSE_CASE('siema', 1);\n";
    CallableStatement callableStatement = connection.prepareCall(sql_update2);
    ResultSet resultSet = callableStatement.executeQuery();
    System.out.println();

    String sql_update3 = "select count(*) from usertable";
    Statement st_4 = connection.createStatement();
    boolean execute3 = st_4.execute(sql_update3);
    boolean execute4 = st_4.execute(sql_update3);
    System.out.println(execute3);
    System.out.println(execute4);


  }
  
}