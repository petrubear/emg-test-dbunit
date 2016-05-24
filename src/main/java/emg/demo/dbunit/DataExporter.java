package emg.demo.dbunit;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.database.search.TablesDependencyHelper;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.ext.oracle.OracleDataTypeFactory;

import javax.sql.DataSource;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.util.Set;

public class DataExporter {
    private DataSource datasource;

    public DataSource getDatasource() {
        return datasource;
    }

    public void setDatasource(DataSource datasource) {
        this.datasource = datasource;
    }

    public DataExporter(DataSource datasource) {
        this.datasource = datasource;
    }

    public void exportTableAndDependencies(String tableName, String fileName, boolean useQualifiedTableNames) throws Exception {
        try (Connection jdbcConnection = datasource.getConnection()) {
            IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);
            DatabaseConfig memConfig = connection.getConfig();
            memConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
            memConfig.setProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, useQualifiedTableNames);
            String[] depTableNames = TablesDependencyHelper.getAllDependentTables(connection, tableName);
            IDataSet depDataset = connection.createDataSet(depTableNames);
            FlatXmlDataSet.write(depDataset, new FileOutputStream(fileName));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void exportSingleTable(String tableName, String fileName, boolean useQualifiedTableNames) throws Exception {
        try (Connection jdbcConnection = datasource.getConnection()) {
            IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);
            DatabaseConfig memConfig = connection.getConfig();
            memConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
            memConfig.setProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, useQualifiedTableNames);
            String[] depTableNames = new String[]{tableName};
            IDataSet depDataset = connection.createDataSet(depTableNames);
            FlatXmlDataSet.write(depDataset, new FileOutputStream(fileName));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void exportFullDatabase(String fileName) throws Exception {
        try (Connection jdbcConnection = datasource.getConnection()) {
            IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);
            DatabaseConfig memConfig = connection.getConfig();
            memConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());

            IDataSet fullDataSet = connection.createDataSet();
            FlatXmlDataSet.write(fullDataSet, new FileOutputStream(fileName));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void exportTableQuery(String table, String sql, String fileName) throws Exception {
        try (Connection jdbcConnection = datasource.getConnection()) {
            IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);
            DatabaseConfig memConfig = connection.getConfig();
            memConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());

            QueryDataSet partialDataSet = new QueryDataSet(connection);
            partialDataSet.addTable(table, sql);
            // partialDataSet.addTable("BAR");
            FlatXmlDataSet.write(partialDataSet, new FileOutputStream(fileName));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void exportTableAndDependencies(String tableName, String fileName, Set allowedIDs, boolean useQualifiedTableNames) throws Exception {
        try (Connection jdbcConnection = datasource.getConnection()) {
            IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);
            DatabaseConfig memConfig = connection.getConfig();
            memConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());
            memConfig.setProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, useQualifiedTableNames);
            //String[] depTableNames = TablesDependencyHelper.getAllDependentTables(connection, tableName);
            IDataSet depDataset = TablesDependencyHelper.getAllDataset(connection, tableName, allowedIDs);
            FlatXmlDataSet.write(depDataset, new FileOutputStream(fileName));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
