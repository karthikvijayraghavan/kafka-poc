package com.couchbase.kafka.kafka_poc.consumer.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.couchbase.kafka.kafka_poc.consumer.bo.CouchbaseEntity;
import com.couchbase.kafka.kafka_poc.consumer.bo.UserProfile;

public class MysqlDAO {

	public static void populateData(CouchbaseEntity couchbaseEntity) {
		try {

			// create a mysql database connection
			String myDriver = "com.mysql.jdbc.Driver";
			String myUrl = "jdbc:mysql://192.168.58.101/user";//?useSSL=false";
			Class.forName(myDriver);
			Connection conn = DriverManager.getConnection(myUrl, "root", "");

			UserProfile userProfile = couchbaseEntity.getUserProfile();
			int idFromCouchbase = Integer
					.valueOf(couchbaseEntity.getKey().substring(couchbaseEntity.getKey().lastIndexOf(":") + 1));

			String selectQuery = "SELECT * from user_profile where id = ? ";
			PreparedStatement selectPreparedStmt = conn.prepareStatement(selectQuery);
			selectPreparedStmt.setInt(1, idFromCouchbase);
			ResultSet rs = selectPreparedStmt.executeQuery();

			if (rs.next()) {
				System.err.println("Record exists in MySQL. Should perform update only");
				String updateQuery = "update  user_profile set first_name=?, last_name=?, is_admin=?, num_points=? where id =?";

				// create the mysql insert preparedstatement
				PreparedStatement updatetPreparedStmt = conn.prepareStatement(updateQuery);
				updatetPreparedStmt.setString(1, userProfile.getFirstName());
				updatetPreparedStmt.setString(2, userProfile.getLastName());
				updatetPreparedStmt.setBoolean(3, userProfile.isAdmin());
				updatetPreparedStmt.setInt(4, userProfile.getNumPoints());
				updatetPreparedStmt.setInt(5, idFromCouchbase);

				// execute the preparedstatement
				updatetPreparedStmt.executeUpdate();
				updatetPreparedStmt.close();
			} else {

				System.err.println("Record Does not existsin MySQL. Should perform Insert operation");
				// the mysql insert statement
				String insertQuery = " insert into user_profile (first_name, last_name, is_admin, num_points,id)"
						+ " values (?, ?,  ?, ?,?)";

				// create the mysql insert preparedstatement
				PreparedStatement insertPreparedStmt = conn.prepareStatement(insertQuery);
				insertPreparedStmt.setString(1, userProfile.getFirstName());
				insertPreparedStmt.setString(2, userProfile.getLastName());
				insertPreparedStmt.setBoolean(3, userProfile.isAdmin());
				insertPreparedStmt.setInt(4, userProfile.getNumPoints());
				insertPreparedStmt.setInt(5, idFromCouchbase);

				// execute the preparedstatement
				insertPreparedStmt.execute();
				insertPreparedStmt.close();
			}
			conn.close();
		} catch (Exception e) {
			System.err.println("Got an exception!");
			System.err.println(e.getMessage());
		}
	}
}