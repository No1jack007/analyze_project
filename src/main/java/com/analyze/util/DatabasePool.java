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
public class DatabasePool implements Serializable {

    private static DatabasePool databasePool = null;
    private static DruidDataSource druidDataSource = null;

    private DatabasePool(String path) {
        Properties properties = loadPropertyFile(path);
        try {
            druidDataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static synchronized DatabasePool getInstance(String path) {
        if (null == databasePool) {
            databasePool = new DatabasePool(path);
        }
        return databasePool;
    }

    public DruidPooledConnection getConnection() throws SQLException {
        return druidDataSource.getConnection();
    }

    public static Properties loadPropertyFile(String fullFile) {
//        String webRootPath = null;
        if (null == fullFile || fullFile.equals("")) {
            throw new IllegalArgumentException("Properties file path can not be null : " + fullFile);
        }
//        webRootPath = DatabasePool.class.getClassLoader().getResource("").getPath();
//        webRootPath = new File(webRootPath).getParent();
        InputStream inputStream = null;
        Properties p = null;
        try {
//            inputStream = new FileInputStream(new File(webRootPath + File.separator + "classes" + File.separator + fullFile));
            inputStream = new FileInputStream(new File(fullFile));
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
