package emg.demo.dbunit;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;

import javax.sql.DataSource;

import org.dbunit.IDatabaseTester;
import org.dbunit.JdbcDatabaseTester;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.hsqldb.jdbc.JDBCDataSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;

public class PersonRepositoryTest {
	private static final String JDBC_DRIVER = org.hsqldb.jdbcDriver.class.getName();
	private static final String JDBC_URL = "jdbc:hsqldb:mem:test.db";// "jdbc:hsqldb:file:./db/test.db";
	private static final String USER = "sa";
	private static final String PASSWORD = "";

	private DataSource dataSource;
	private static IDatabaseTester databaseTester;

	@BeforeClass
	public static void createSchema() throws Exception {
		databaseTester = new JdbcDatabaseTester(JDBC_DRIVER, JDBC_URL, USER, PASSWORD);
		try (Connection con = databaseTester.getConnection().getConnection()) {
			String sql = "create table if not exists PERSON (ID int primary key, NAME varchar(20), LAST_NAME varchar(20), AGE  int)";
			PreparedStatement ps = con.prepareStatement(sql);
			ps.execute();
			ps.close();
		}
	}

	@AfterClass
	public static void tearDownClass() {
		// Close session etc.
	}

	@Before
	public void setUp() throws Exception {
		IDataSet dataSet = readDataSet();
		databaseTester.setSetUpOperation(DatabaseOperation.CLEAN_INSERT);
		databaseTester.setDataSet(dataSet);
		databaseTester.onSetup();
	}

	@After
	public void tearDown() throws Exception {
		databaseTester.onTearDown();
	}

	private IDataSet readDataSet() throws MalformedURLException, DataSetException, URISyntaxException {
		URL resource = PersonRepositoryTest.class.getClassLoader().getResource("dataset.xml");
		IDataSet dataSet = new FlatXmlDataSetBuilder().build(Paths.get(resource.toURI()).toFile());
		return dataSet;
	}

	private DataSource dataSource() {
		JDBCDataSource dataSource = new JDBCDataSource();
		dataSource.setURL(JDBC_URL);
		dataSource.setUser(USER);
		dataSource.setPassword(PASSWORD);
		return dataSource;
	}

	@Test
	public void findsAndReadsExistingPersonByFirstName() throws Exception {
		PersonRepository repository = new PersonRepository(dataSource());
		Person charlie = repository.findPersonByFirstName("Charlie");

		Assert.assertEquals(charlie.getAge(), 42);
	}

	@Test
	public void insertTest() {
		Person person = new Person();
		person.setId(0);
		person.setFirstName("edison");
		person.setLastName("martinez");
		person.setAge(30);

		PersonRepository repository = new PersonRepository(dataSource());
		try {
			Object[] r = repository.insertPerson(person);
			Assert.assertNull(r);
		} catch (Exception ex) {
		}
	}

	@Test
	public void insertTest2() {
		Person person = new Person();
		person.setId(0);
		person.setFirstName("edison");
		person.setLastName("martinez");
		person.setAge(30);

		PersonRepository repository = new PersonRepository(dataSource());
		try {
			Object[] r = repository.insertPerson(person);
			Assert.assertNull(r);
		} catch (Exception ex) {
		}
	}

}
