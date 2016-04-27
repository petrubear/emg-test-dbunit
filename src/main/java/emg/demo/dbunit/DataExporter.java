package emg.demo.dbunit;

import java.io.FileOutputStream;
import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.database.search.TablesDependencyHelper;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;

public class DataExporter {
	public void exportData(DataSource datasource) throws Exception {
		// database connection
		// Class driverClass = Class.forName("org.hsqldb.jdbcDriver");
		// Connection jdbcConnection =
		// DriverManager.getConnection("jdbc:hsqldb:sample", "sa", "");
		Connection jdbcConnection = datasource.getConnection();
		IDatabaseConnection connection = new DatabaseConnection(jdbcConnection);

		try {
			// partial database export
//			QueryDataSet partialDataSet = new QueryDataSet(connection);
//			partialDataSet.addTable("FOO", "select * from tcrb_order");
//			partialDataSet.addTable("BAR");
//			FlatXmlDataSet.write(partialDataSet, new FileOutputStream("partial.xml"));

			// full database export
			//IDataSet fullDataSet = connection.createDataSet();
			//FlatXmlDataSet.write(fullDataSet, new FileOutputStream("full.xml"));

			// dependent tables database export: export table X and all tables
			// that
			// have a PK which is a FK on X, in the right order for insertion
			String[] depTableNames = TablesDependencyHelper.getAllDependentTables(connection, "TCRB_ORDER");
			IDataSet depDataset = connection.createDataSet(depTableNames);
			FlatXmlDataSet.write(depDataset, new FileOutputStream("dependents.xml"));
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
