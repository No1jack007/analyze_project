package com.analyze.util;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;

import javax.sql.DataSource;
import java.io.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author: zhang yufei
 * @create: 2020-06-12 11:22
 **/
public class DatabasePool {

    private static DatabasePool databasePool = null;
    private static DruidDataSource druidDataSource = null;

    static {
        Properties properties = loadPropertyFile("db.properties");
        try {
            druidDataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DatabasePool() {
    }

    public static synchronized DatabasePool getInstance() {
        if (null == databasePool) {
            databasePool = new DatabasePool();
        }
        return databasePool;
    }

    public DruidPooledConnection getConnection() throws SQLException {
        return druidDataSource.getConnection();
    }

    public static Properties loadPropertyFile(String fullFile) {
        String webRootPath = null;
        if (null == fullFile || fullFile.equals("")) {
            throw new IllegalArgumentException("Properties file path can not be null : " + fullFile);
        }
        webRootPath = DatabasePool.class.getClassLoader().getResource("").getPath();
        webRootPath = new File(webRootPath).getParent();
        InputStream inputStream = null;
        Properties p = null;
        try {
            inputStream = new FileInputStream(new File(webRootPath + File.separator + "classes" + File.separator + fullFile));
            p = new Properties();
            p.load(inputStream);
        } catch (FileNotFoundException e) {
            throw new IllegalArgumentException("Properties file not found: " + fullFile);
        } catch (IOException e) {
            throw new IllegalArgumentException("Properties file can not be loading: " + fullFile);
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return p;
    }


}
