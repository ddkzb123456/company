package com.yjt.datashare.utils;

import com.jcraft.jsch.*;

import java.io.*;
import java.sql.*;
import java.util.Vector;

public class DataUtils {

    // 数据查询方法，返回ResultSet对象
    public static ResultSet executeQuery(Connection connection, String tableName, String where, String limit) throws SQLException {
        String sql = "SELECT * FROM " + tableName;
        if (where != null && !where.isEmpty()) {
            sql += " WHERE " + where;
        }
        if (limit != null && !limit.isEmpty()) {
            sql += " LIMIT " + limit;
        }

        Statement statement = connection.createStatement();
        return statement.executeQuery(sql);
    }


    // 数据导出为Shapefile方法
    public static void exportAndDownloadShapefiles(String host,String systemUser,String systemPassword,int sysport,
                                                   String dbUser, String dbPassword, int dbport, String database,String query,
                                                   String outputFilePath, String remoteDirectory, String localDirectory) {
        try {
            // 1. Execute pgsql2shp command remotely
            executeRemotePsql2shpCommand(host, dbUser, dbPassword, dbport,
                    systemUser,systemPassword,sysport,database, query,remoteDirectory,outputFilePath);

            // 2. Download Shapefile files
            downloadFiles(host, systemUser, systemPassword, sysport, remoteDirectory, localDirectory);

            System.out.println("Shapefile exported and downloaded successfully!");

        } catch (JSchException | IOException | InterruptedException | SftpException e) {
            e.printStackTrace();
        }
    }

    public static void executeRemotePsql2shpCommand(String host, String username, String password, int port,
                                                    String systemUser,String systemPassword,int sysport,
                                                    String database, String query,String remoteDirectory,String outputFilePath)
            throws IOException, JSchException, InterruptedException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(systemUser,host,sysport);
        session.setPassword(systemPassword);

        // Disable strict host key checking
        java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        session.connect();

        ChannelExec channelExec1 = (ChannelExec) session.openChannel("exec");

        String mkdirCommand = "mkdir -p " + remoteDirectory; // -p flag creates parent directories if they don't exist
        channelExec1.setCommand(mkdirCommand);
        channelExec1.connect();
        //waitForCommandCompletion(channelExec1);


        // Execute pgsql2shp command
        ChannelExec channelExec2 = (ChannelExec) session.openChannel("exec");

        String command = "pgsql2shp -f " + outputFilePath +" "+"-h"+ host + " -u " + username + " -P " + password + " -p " + port + " " + database + " " + "\""+query+"\"";
        System.out.println(command);
        channelExec2.setCommand(command);
        channelExec2.connect();

        InputStream inputStream = channelExec2.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        //waitForCommandCompletion(channelExec);


        channelExec1.disconnect();
        channelExec2.disconnect();
        session.disconnect();
    }



    public static void downloadFiles(String host, String username, String password, int port,
                                     String remoteDirectory, String localDirectory) throws JSchException, IOException, SftpException {
        new File(localDirectory).mkdirs();
        JSch jsch = new JSch();
        Session session;
        ChannelSftp channelSftp;

        session = jsch.getSession(username, host, port);
        session.setPassword(password);

        // Disable strict host key checking
        java.util.Properties config = new java.util.Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);

        session.connect();

        Channel channel = session.openChannel("sftp");
        channel.connect();

        channelSftp = (ChannelSftp) channel;
        channelSftp.cd(remoteDirectory);

        // List files in remote directory
        Vector<ChannelSftp.LsEntry> filelist = channelSftp.ls(remoteDirectory);
        for (ChannelSftp.LsEntry file : filelist) {
            if (!file.getAttrs().isDir()) {
                // Download file from remote directory and delete file which was got
                String remoteFile = remoteDirectory + "/" + file.getFilename();
                String localFile = localDirectory + "/" + file.getFilename();
                channelSftp.get(remoteFile, localFile);
                channelSftp.rm(remoteFile);
            }
        }
        // delete filedir
        channelSftp.rmdir(remoteDirectory);
        channelSftp.disconnect();
        session.disconnect();
    }

    public static void waitForCommandCompletion(ChannelExec channelExec) throws InterruptedException {
        while (true) {
            if (channelExec.isClosed()) {
                break;
            }
            Thread.sleep(1000);
        }
        int exitStatus = channelExec.getExitStatus();
        System.out.println("Exit Status: " + exitStatus);
    }




    // 数据导出为CSV方法
    public static void exportToCSV(ResultSet resultSet,String csvFileDir,String csvFileName) throws SQLException, IOException {
        String csvFilePath = csvFileDir + csvFileName;
        new File(csvFileDir).mkdirs();
        FileWriter writer = new FileWriter(csvFilePath);
        ResultSetMetaData metadata = resultSet.getMetaData();
        int columnCount = metadata.getColumnCount();

// 写入CSV文件的表头
        for (int i = 1; i <= columnCount; i++) {
            writer.append(quote(metadata.getColumnName(i)));
            if (i != columnCount) {
                writer.append(",");
            }
        }
        writer.append(System.lineSeparator());

// 写入CSV文件的数据行
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                Object value = resultSet.getObject(i);
                if (value != null) {
                    writer.append(quote(value.toString()));
                }
                if (i != columnCount) {
                    writer.append(",");
                }
            }
            writer.append(System.lineSeparator());
        }

        writer.flush();
        writer.close();


    }

    // 辅助方法：为包含特殊字符的字段值添加双引号
    private static String quote(String value) {
        if (value.contains("\"")) {
            value = value.replace("\"", "\"\"");
        }
        return "\"" + value + "\"";
    }


}