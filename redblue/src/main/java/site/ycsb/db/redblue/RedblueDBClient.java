/*
 * Copyright 2017 YCSB Contributors. All Rights Reserved.
 *
 * CODE IS BASED ON the jdbc-binding JdbcDBClient class.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db.redblue;

import site.ycsb.*;
import org.json.simple.JSONObject;
import org.postgresql.Driver;
import org.postgresql.util.PGobject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;

import java.util.*;

/**
 * redblue client for YCSB framework.
 */
public class RedblueDBClient extends DB {
  private static final Logger LOG = LoggerFactory.getLogger(RedblueDBClient.class);

  /**
   * Cache for already prepared statements.
   */
  private ConcurrentMap<StatementType, PreparedStatement> cachedStatements;
  private float redFailureProbability;
  private Connection connection;

  /**
   * The URL to connect to the database.
   */
  public static final String CONNECTION_URL = "redblue.url";

  /**
   * The username to use to connect to the database.
   */
  public static final String CONNECTION_USER = "redblue.user";

  /**
   * The password to use for establishing the connection.
   */
  public static final String CONNECTION_PASSWD = "redblue.passwd";

  public static final String CONNECTION_RED_FAILURE = "redblue.red.failure-prob";

  /**
   * The primary key in the user table.
   */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /**
   * The field name prefix in the table.
   */
  public static final String COLUMN_NAME = "YCSB_VALUE";

  private static final String DEFAULT_PROP = "";

  /**
   * Returns parsed boolean value from the properties if set, otherwise returns defaultVal.
   */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  @Override
  public void init() throws DBException {
    Properties props = getProperties();
    String hostsString = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    redFailureProbability = Float.parseFloat(props.getProperty(CONNECTION_RED_FAILURE, "0.0"));

    try {
      Properties tmpProps = new Properties();
      tmpProps.setProperty("user", user);
      tmpProps.setProperty("password", passwd);

      cachedStatements = new ConcurrentHashMap<>();
      List<String> hosts = Arrays.asList(hostsString.split(","));
      String host = hosts.get(ThreadLocalRandom.current().nextInt(hosts.size()));
      Driver driver = new Driver();
      tmpProps.setProperty("options", "-c statement_timeout=90000");
      LOG.info("Thread-{} Connecting to {}", Thread.currentThread().getName(), host);
      this.connection = driver.connect(host, tmpProps);


    } catch (Exception e) {
      LOG.error("Error during initialization: " + e);
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      cachedStatements.clear();
      connection.close();
    } catch (SQLException e) {
      System.err.println("Error in cleanup execution. " + e);
    }
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, fields);
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type);
      }
      readStatement.setString(1, key);
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(readStatement.toString());
      if (!resultSet.next()) {
        resultSet.close();
        return  Status.NOT_FOUND;
      }

      if (result != null) {
        if (fields == null){
          do{
            String field = resultSet.getString(2);
            String value = resultSet.getString(3);
            result.put(field, new StringByteIterator(value));
          }while (resultSet.next());
        } else {
          for (String field : fields) {
            String value = resultSet.getString(field);
            result.put(field, new StringByteIterator(value));
          }
        }
      }
      resultSet.close();
      return Status.OK;

    } catch (SQLException e) {
      LOG.error("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, fields);
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type);
      }
      scanStatement.setString(1, startKey);
      scanStatement.setInt(2, recordcount);
      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(scanStatement.toString());
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            values.put(field, new StringByteIterator(value));
          }

          result.add(values);
        }
      }
      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error in processing scan of table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try{
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, null);
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type);
      }

      updateStatement.setString(1, key);
      updateStatement.setFloat(2, redFailureProbability);

      Statement statement = connection.createStatement();
      int result = statement.executeUpdate(updateStatement.toString());
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    PreparedStatement insertStatement = null;
    try{
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName, null);
      insertStatement = connection.prepareStatement(createInsertStatement(type));

      JSONObject jsonObject = new JSONObject();
      jsonObject.put("field1", UUID.randomUUID().toString());

      insertStatement.setObject(2, jsonObject.toJSONString());
      insertStatement.setString(1, key);


      Statement statement = connection.createStatement();
      int result = statement.executeUpdate(insertStatement.toString());
      if (result == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing insert to table: " + tableName + ": " + e);
      LOG.error("String insertStatement={}, String key={}, Map<String, ByteIterator> values={}", insertStatement, key, values);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try{
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, null);
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type);
      }
      deleteStatement.setString(1, key);

      Statement statement = connection.createStatement();
      int result = statement.executeUpdate(deleteStatement.toString());
      if (result == 1){
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType)
      throws SQLException{
    PreparedStatement readStatement = connection.prepareStatement(createReadStatement(readType));
    PreparedStatement statement = cachedStatements.putIfAbsent(readType, readStatement);
    if (statement == null) {
      return readStatement;
    }
    return statement;

  }

  private String createReadStatement(StatementType readType){
    StringBuilder read = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);

    if (readType.getFields() == null) {
      read.append(", (jsonb_each_text(" + COLUMN_NAME + ")).*");
    } else {
      for (String field:readType.getFields()){
        read.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }

    read.append(" FROM " + readType.getTableName());
    read.append(" WHERE ");
    read.append(PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    return read.toString();
  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType)
      throws SQLException{
    PreparedStatement scanStatement = connection.prepareStatement(createScanStatement(scanType));
    PreparedStatement statement = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (statement == null) {
      return scanStatement;
    }
    return statement;
  }

  private String createScanStatement(StatementType scanType){
    StringBuilder scan = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);
    if (scanType.getFields() != null){
      for (String field:scanType.getFields()){
        scan.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }
    scan.append(" FROM " + scanType.getTableName());
    scan.append(" WHERE ");
    scan.append(PRIMARY_KEY);
    scan.append(" >= ?");
    scan.append(" ORDER BY ");
    scan.append(PRIMARY_KEY);
    scan.append(" LIMIT ?");

    return scan.toString();
  }

  public PreparedStatement createAndCacheUpdateStatement(StatementType updateType)
      throws SQLException{
    PreparedStatement updateStatement = connection.prepareStatement(createUpdateStatement(updateType));
    PreparedStatement statement = cachedStatements.putIfAbsent(updateType, updateStatement);
    if (statement == null) {
      return updateStatement;
    }
    return statement;
  }

  private String createUpdateStatement(StatementType updateType){
    return "CALL REVERSE_CASE(?, ?);";
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType)
      throws SQLException {
    PreparedStatement insertStatement = connection.prepareStatement(createInsertStatement(insertType));
    PreparedStatement statement = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (statement == null) {
      return insertStatement;
    }
    return statement;
  }

  private String createInsertStatement(StatementType insertType){
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(insertType.getTableName());
    insert.append(" (" + PRIMARY_KEY + "," + COLUMN_NAME + ")");
    insert.append(" VALUES(?,?)");
    return insert.toString();
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType)
      throws SQLException{
    PreparedStatement deleteStatement = connection.prepareStatement(createDeleteStatement(deleteType));
    PreparedStatement statement = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (statement == null) {
      return deleteStatement;
    }
    return statement;
  }

  private String createDeleteStatement(StatementType deleteType){
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append(PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }
}
