package emg.demo.dbunit;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.Test;

import javax.sql.DataSource;
import java.util.HashSet;
import java.util.Set;

public class DataExporterTest {
    @Test
    public void testExportTableQuery() throws Exception {
        DataExporter dex = new DataExporter(eappfisaDataSource());
        final String table = "UniversalCatalog";
        final String sql = "SELECT UCT.FIELD_NAME AS FIELD_NAME, UCT.TARGET_TABLE AS T_TABLE, "
                + "UCT.TARGET_FIELD AS T_FIELD, SSY.DATA_SOURCE_ID AS DATA_SOURCE, "
                + "UCT.APPLICATION_ID AS APPLICATION_ID, " + "ELE.NAME AS ELEMENT_NAME, " + "FMT.NAME AS FORMAT_NAME, "
                + "REG.NAME REGISTER_NAME " + "FROM " + "TPAR_UNIVERSAL_CATALOG UCT, " + "TAPD_APPLICATION APP, "
                + "TAPD_SOURCE_SYSTEM SSY, " + "TPAR_ELEMENT ELE, " + "TPAR_REGISTER REG, " + "TPAR_FILE_FORMAT FMT "
                + "WHERE " + "UCT.APPLICATION_ID = APP.APPLICATION_ID "
                + "AND APP.SOURCE_SYSTEM_ID = SSY.SOURCE_SYSTEM_ID " + "AND UCT.FIELD_NAME = ELE.FIELD_NAME "
                + "AND REG.ID_REGISTER = ELE.ID_REGISTER " + "AND FMT.ID_FILE_FORMAT = REG.ID_FILE_FORMAT";

        final String fileName = "/Users/edison/Tmp/export/ucatalog.xml";
        dex.exportTableQuery(table, sql, fileName);
    }

    @Test
    public void testExportSingleTAble() throws Exception {
        DataExporter dex = new DataExporter(eappfisaDataSource());
        final String tableName = "EAPPFISA_V38.TCRB_TEMPLATE_SERVICE";
        final String fileName = "/Users/edison/Tmp/export/TCRB_TEMPLATE_SERVICE.xml";
        dex.exportSingleTable(tableName, fileName, true);
    }

    @Test
    public void testExportTableAndDependencies() throws Exception {
        DataExporter dex = new DataExporter(eappfisaDataSource());
		final String tableName = "TCRB_ORDER";
		final String fileName = "/Users/edison/Tmp/export/tcrb_order.xml";
        dex.exportTableAndDependencies(tableName, fileName, false);
    }

    @Test
    public void testExportTableAndDependenciesSet() throws Exception {
        DataExporter dex = new DataExporter(eappfisaDataSource());
        Set<String> set = new HashSet<>();
        final String tableName = "TCRB_TEMPLATE_SERVICE";
        final String fileName = "/Users/edison/Tmp/export/TCRB_TEMPLATE_SERVICE.xml";
        set.add("1");
        dex.exportTableAndDependencies(tableName, fileName, set, false);
    }

    private DataSource eappfisaDataSource() {
        final String ORA_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
        final String BASE_URL = "jdbc:oracle:thin:@";

        final String ip = "192.168.0.164";
        final String serviceName = "modinter";
        final String port = "1521";
        final String userName = "EAPPFISA_V38";
        final String password = "eappfisauni38";

        String url = BASE_URL.concat(ip).concat(":").concat(port).concat(":").concat(serviceName);
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(ORA_DRIVER_CLASS);
        ds.setUrl(url);
        ds.setUsername(userName);
        ds.setPassword(password);

        return ds;
    }

    private DataSource eefisaDataSource() {
        final String ORA_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
        final String BASE_URL = "jdbc:oracle:thin:@";

        final String ip = "192.168.0.164";
        final String serviceName = "modinter";
        final String port = "1521";
        final String userName = "EEFISA_V38";
        final String password = "eefisauni38";

        String url = BASE_URL.concat(ip).concat(":").concat(port).concat(":").concat(serviceName);
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(ORA_DRIVER_CLASS);
        ds.setUrl(url);
        ds.setUsername(userName);
        ds.setPassword(password);

        return ds;
    }

}
