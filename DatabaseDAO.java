package com.yjt.datashare.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DatabaseDAO {
    private  Connection connection;

    // 构造方法
        public DatabaseDAO(String host, int port, String database, String username, String password) throws SQLException {
        String url = "jdbc:kingbase8://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.kingbase8.Driver");
            connection = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 获取连接
    public Connection getConnection() {
        return connection;
    }

    // 关闭连接
    public void closeConnection() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
