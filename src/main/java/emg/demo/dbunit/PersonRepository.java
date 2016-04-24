package emg.demo.dbunit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.DataSource;

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

}
