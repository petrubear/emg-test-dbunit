package emg.demo.dbunit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

public class PersonRepository {
	private DataSource datasource;

	public PersonRepository(DataSource datasource) {
		this.datasource = datasource;
	}

	public Person findPersonByFirstName(String name) {
		Person person = null;
		try (Connection connection = this.datasource.getConnection()) {
			String sql = "Select NAME, LAST_NAME, AGE from PERSON where NAME = ?";
			PreparedStatement ps = connection.prepareStatement(sql);
			ps.setString(1, name);
			ResultSet rs = ps.executeQuery();
			while (rs.next()) {
				person = new Person();
				person.setFirstName(rs.getString(1));
				person.setLastName(rs.getString(2));
				person.setAge(rs.getInt(3));
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return person;
	}

	public Object[]  insertPerson(Person person) throws Exception {
		final String sql = "insert into PERSON (ID, NAME, LAST_NAME, AGE) values (?,?,?,?)";
		Object[] params = { person.getId(), person.getFirstName(), person.getLastName(), person.getAge() };
		QueryRunner runner = new QueryRunner();
		Object[] inResult = null;
		try (Connection conn = datasource.getConnection()) {
			// result = runner.update(conn, sql, params);
			ResultSetHandler<Object[]> h = new ResultSetHandler<Object[]>() {
				public Object[] handle(ResultSet rs) throws SQLException {
					if (!rs.next()) {
						return null;
					}

					ResultSetMetaData meta = rs.getMetaData();
					int cols = meta.getColumnCount();
					Object[] result = new Object[cols];

					for (int i = 0; i < cols; i++) {
						result[i] = rs.getObject(i + 1);
					}

					return result;
				}
			};

			inResult = runner.insert(conn, sql, h, params);

		} catch (Exception ex) {
			ex.printStackTrace();
		}

		return inResult;
	}
}
