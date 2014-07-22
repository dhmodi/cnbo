package com.deloitte.datasource;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;

import com.deloitte.cnbo.util.ApplicationConstants;

public class DataSource {

    private static DataSource     datasource;
    private BasicDataSource dataSource;
    
    private DataSource(Properties dbConfig) throws IOException, SQLException, PropertyVetoException {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(dbConfig.getProperty(ApplicationConstants.DB_DRIVER_CLASS));
        dataSource.setUsername(dbConfig.getProperty(ApplicationConstants.DB_USER_NAME));
        dataSource.setPassword(dbConfig.getProperty(ApplicationConstants.DB_PASSWORD));
        dataSource.setUrl(dbConfig.getProperty(ApplicationConstants.DB_CONNECTION_URL));
       
        dataSource.setInitialSize(10);
        dataSource.setMaxActive(10);
        dataSource.setMinIdle(5);
        dataSource.setMaxIdle(20);
        dataSource.setMaxOpenPreparedStatements(180);

    }

    public static DataSource getInstance(Properties dbConfig) throws IOException, SQLException, PropertyVetoException {
        if (datasource == null) {
            datasource = new DataSource(dbConfig);
            return datasource;
        } else {
            return datasource;
        }
    }

    public Connection getConnection() throws SQLException {
        return this.dataSource.getConnection();
    }

}