package emg.demo.dbunit;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;

public class DataExporterTest {

	@Test
	public void exportDataTest() throws Exception {
		DataExporter dex = new DataExporter();
		dex.exportData(getDataSource());
	}

	private DataSource getDataSource() {
		final String ORA_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
		final String BASE_URL = "jdbc:oracle:thin:@";

		final String ip = "192.168.0.164";
		final String serviceName = "modinter";
		final String port = "1521";
		final String userName = "EAPPFISA_V38";
		final String password = "EAPPFISA_V38";

		String url = BASE_URL.concat(ip).concat(":").concat(port).concat(":").concat(serviceName);
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(ORA_DRIVER_CLASS);
		ds.setUrl(url);
		ds.setUsername(userName);
		ds.setPassword(password);

		return ds;
	}
}
