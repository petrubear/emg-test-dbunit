package emg.demo.dbunit;

import java.io.FileOutputStream;
import java.sql.Connection;

import javax.sql.DataSource;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.database.search.TablesDependencyHelper;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.ext.oracle.OracleDataTypeFactory;

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

	public void exportTableAndDependencies(String tableName, String fileName) throws Exception {
		try (Connection jdbcConnection = datasource.getConnection()) {
			IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);
			DatabaseConfig memConfig = connection.getConfig();
			memConfig.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new OracleDataTypeFactory());

			String[] depTableNames = TablesDependencyHelper.getAllDependentTables(connection, tableName);
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
}
