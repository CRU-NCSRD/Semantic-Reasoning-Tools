package eu.c2learn.crawlers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
//import java.sql.Types;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;

public class NomadCrawlerUtils {
	
	static String workingDir = "";
	
	public static void initialiseUtils(String setWorkingDir) {
		workingDir = setWorkingDir;
	}

	public static String identifyLanguage(String text) {
		String language = "en";
		try {// detect the language of the given text
			DetectorFactory.loadProfile(workingDir + "profiles");
			Detector detector = DetectorFactory.create();
			detector.append(text);
			language = detector.detect();	
		} catch (LangDetectException e) {
			if ( ! e.getMessage().equals("no features in text")) {
				e.printStackTrace();
				DetectorFactory.clear();
			}		
		}
		DetectorFactory.clear();
		return language;
	}
        
        public static int storeNomadContent(Connection dbConnection, String url, String clean_text, String timestamp, String language, String sourceTable, long queryId) throws SQLException, ParseException {
			int countResults = 0;

				if (clean_text == null || clean_text.isEmpty()) {
					return 0;
				}
				
				Long id = getLatestContentId(dbConnection, url, queryId, sourceTable);
				if (id == null  || id == 0) {
					countResults = simpleStoreContent(dbConnection, url, clean_text, timestamp, language, sourceTable, queryId);
				} else {
					if ( ! contentHashIsEqual(dbConnection, id, clean_text)) {
						Date dateFromDB = retrieveLatestDateFromDB(dbConnection, id);
						Date dateFormated = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(timestamp);
						if (dateFormated.after(dateFromDB)) {
							//countResults = updateContent(dbConnection, id, clean_text, timestamp, language);
							countResults = simpleStoreContent(dbConnection, url, clean_text, timestamp, language, sourceTable, queryId);
						}
					}
				}	
			return countResults;
		}
		
		
		private static int simpleStoreContent(Connection dbConnection, String url, String clean_text, String timestamp, String language, String sourceTable, long queryId) throws SQLException {
			if (clean_text == null || clean_text.isEmpty()) {
				return 0;
			}
			
			String sql = "INSERT INTO nomad_content (url, clean_text, timestamp, language, source_table, query_id) " +
					"VALUES (?, ?, ?, ?, ?, ?);";
			int countResults = 0;
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = dbConnection.prepareStatement(sql);
				preparedStatement.setString(1, url);
				preparedStatement.setString(2, clean_text);
				preparedStatement.setString(3, timestamp);
				preparedStatement.setString(4, language);
				preparedStatement.setString(5, sourceTable);
				preparedStatement.setLong(6, queryId);
				countResults = preparedStatement.executeUpdate();
			} finally {
				if (preparedStatement!=null) {
					preparedStatement.close();
				}
			}
			/*} catch (SQLException e) {
				e.printStackTrace();
			}*/
			return countResults;
		}
		
		
		private static Date retrieveLatestDateFromDB (Connection dbConnection, Long id) throws SQLException {
			String sql = "SELECT timestamp FROM nomad_content WHERE id = ?;";
			Date date = null;
			//try {
				PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
				preparedStatement.setLong(1, id);
				preparedStatement.execute();
				ResultSet resultSet = preparedStatement.getResultSet();
				if (resultSet.next()) {
					date = resultSet.getTimestamp("timestamp");
				}
				preparedStatement.close();
			/*} catch (SQLException e) {
				e.printStackTrace();
			}*/
			return date;
		}
		
		/*
		public static int updateContent(Connection dbConnection, Long id, String clean_text, String timestamp, String language) throws SQLException {
			if (clean_text == null || clean_text.isEmpty()) {
				return 0;
			}
			String sql = "UPDATE nomad_content SET clean_text=?, timestamp=?, language=? WHERE id=?;";
			int countResults = 0;
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = dbConnection.prepareStatement(sql);
				preparedStatement.setString(1, clean_text);
				preparedStatement.setString(2, timestamp);
				preparedStatement.setString(3, language);
				preparedStatement.setLong(4, id);
				countResults = preparedStatement.executeUpdate();
			} finally {
				if (preparedStatement!=null) {
					preparedStatement.close();
				}
			}
			return countResults;
		}
		*/
	
		private static boolean contentHashIsEqual(Connection dbConnection, Long id, String content) throws SQLException {
			boolean isEqual = false;
			PreparedStatement preparedStatement = null;
			//try {
				String sql = "SELECT MD5(clean_text) FROM nomad_content WHERE id = ?";
				preparedStatement = dbConnection.prepareStatement(sql);
				preparedStatement.setLong(1, id);
				preparedStatement.execute();
				ResultSet resultSet = preparedStatement.getResultSet();
				while (resultSet.next()) {
					String dbHash = resultSet.getString("MD5(clean_text)");
					if (dbHash.equals(DigestUtils.md5Hex(content))) {
						isEqual =  true;
					}
				}
				preparedStatement.close();
			//} finally {
				
			//}
			return isEqual;
		}
		

	
	
	private static Long getLatestContentId(Connection dbConnection, String url, long queryId, String sourceTable ) throws SQLException {
		Long id = null;
		PreparedStatement preparedStatement = null;
		String sql = "SELECT id FROM nomad_content " +
				"WHERE url LIKE ? AND query_id = ? AND source_table LIKE ? " +
				"AND timestamp = (SELECT MAX(timestamp) FROM nomad_content WHERE url LIKE ? AND query_id = ? AND source_table LIKE ?);";
		try {
			preparedStatement = dbConnection.prepareStatement(sql);
			preparedStatement.setString(1, url);
			preparedStatement.setLong(2, queryId);
			preparedStatement.setString(3, sourceTable);
			preparedStatement.setString(4, url);
			preparedStatement.setLong(5, queryId);
			preparedStatement.setString(6, sourceTable);
			preparedStatement.execute();
			ResultSet resultSet = preparedStatement.getResultSet();
			if (resultSet.next()) {
				id = resultSet.getLong("id");
			}
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		return id;
		
	}
	
	
	public static long getNomadContentId(Connection dbConnection, String url, String timestamp, String sourceTable, long queryId) throws SQLException {
		long id = 0L;
		String sql = "SELECT id FROM nomad_content WHERE url LIKE ? AND timestamp=? AND source_table LIKE ? AND query_id=?;";
		//String sql = "SELECT id FROM nomad_content WHERE source_table = ? AND source_id = ?;";
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = dbConnection.prepareStatement(sql);
			preparedStatement.setString(1, url);
			preparedStatement.setString(2, timestamp);
			preparedStatement.setString(3, sourceTable);
			preparedStatement.setLong(4, queryId);
			preparedStatement.execute();
			ResultSet resultSet = preparedStatement.getResultSet();
			if (resultSet.next()) {
				id = resultSet.getLong("id");
			}
			preparedStatement.close();
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		return id;
	}
	
	
	public static int storeAge(Connection dbConnection, long nomadContentId, Integer age) {
		int countResults = 0;
		if (age != null && age>=18 && age<=64) {
			String ageColumn = "";
			if (age<=24) {
				ageColumn = "age18to24";
			} else if (age<=34) {
				ageColumn = "age25to34";
			} else if (age<=44) {
				ageColumn = "age35to44";
			} else if (age<=54) {
				ageColumn = "age45to54";
			} else {
				ageColumn = "age55to64";
			}
			String sql = "INSERT INTO nomad_demographics (nomad_content_id, " + ageColumn + ") " +
					"VALUES(?,1) " +
					"ON DUPLICATE KEY UPDATE " +
					"age18to24=0, age25to34=0, age35to44=0, age45to54=0, age55to64 =0," +
					" " + ageColumn + "=1;";
			try {
				PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
				preparedStatement.setLong(1, nomadContentId);
				countResults = preparedStatement.executeUpdate();
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		} 
		return countResults;
	}
	
	
	public static int storeEducation(Connection dbConnection, long nomadContentId, String education) {
		int countResults = 0;
		if (education != null) {
			String sql = "INSERT INTO nomad_demographics (nomad_content_id, college, grad_school) " +
					"VALUES(?, 0.75, 0.25) " +
					"ON DUPLICATE KEY UPDATE no_college=0, some_college=0, college=0.75, grad_school=0.25;";
			try {
				PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
				preparedStatement.setLong(1, nomadContentId);
				countResults = preparedStatement.executeUpdate();
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}			
		} 
		return countResults;
	}
	
	
	public static int storeGender(Connection dbConnection, long nomadContentId, String gender) {
		int countResults = 0;
		if (gender != null) {
			String genderColumn = "";
			if (gender.equalsIgnoreCase("male")) {
				genderColumn = "male";
			} else if (gender.equalsIgnoreCase("female")) {
				genderColumn = "female";
			} else {
				return 0;
			}
			String sql = "INSERT INTO nomad_demographics (nomad_content_id, " + genderColumn + ") " +
					"VALUES(?,1) " +
					"ON DUPLICATE KEY UPDATE " +
					"male=0, female=0, " + genderColumn + "=1;";
			try {
				PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
				preparedStatement.setLong(1, nomadContentId);
				countResults = preparedStatement.executeUpdate();
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
			
		} 
		return countResults;
	}
	

	public static int storeLocation(Connection dbConnection, long nomadContentId, String regionName) {
		String sql = "INSERT INTO nomad_demographics (nomad_content_id, region_name) " +
				"VALUES (?, ?) " +
				"ON DUPLICATE KEY UPDATE region_name = ?;";
		int countResults = 0;
		try {
			PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
			preparedStatement.setLong(1, nomadContentId);
			preparedStatement.setString(2, regionName);
			preparedStatement.setString(3, regionName);
			countResults = preparedStatement.executeUpdate();
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return countResults;
	}
	
/*	
	public static int storeDemographics(Connection dbConnection, long nomadContentId, Integer age, String gender, String education, String regionName, String geonamesId) {	
		String sql = "INSERT INTO nomad_demographics (nomad_content_id, age, gender, education, region_name, geonames_id) " +
				"VALUES (?, ?, ?, ?, ?, ?) " +
				"ON DUPLICATE KEY UPDATE age = ?, gender = ?, education = ?, region_name = ?, geonames_id = ?;";
		int countResults = 0;
		try {
			PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
			preparedStatement.setLong(1, nomadContentId);
			if (age == null || age <= 0) {
				preparedStatement.setNull(2, Types.SMALLINT);
			} else {	
				preparedStatement.setInt(2, age);
			}
			preparedStatement.setString(3, gender);
			preparedStatement.setString(4, education);
			preparedStatement.setString(5, regionName);
			preparedStatement.setString(6, geonamesId);
			if (age == null || age <= 0) {
				preparedStatement.setNull(7, Types.SMALLINT);
			} else {
				preparedStatement.setInt(7, age);
			}
			preparedStatement.setString(8, gender);
			preparedStatement.setString(9, education);
			preparedStatement.setString(10, regionName);
			preparedStatement.setString(11, geonamesId);
			countResults = preparedStatement.executeUpdate();
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return countResults;
	}
*/
	/*
	public static long getNomadContentId(Connection dbConnection, String sourceTable, long sourceId) {
		long id = 0L;
		String sql = "SELECT id FROM nomad_content WHERE source_table = ? AND source_id = ?;";
		try {
			PreparedStatement preparedStatement = dbConnection.prepareStatement(sql);
			preparedStatement.setString(1, sourceTable);
			preparedStatement.setLong(2, sourceId);
			preparedStatement.execute();
			ResultSet resultSet = preparedStatement.getResultSet();
			if (resultSet.next()) {
				id = resultSet.getLong("id");
			}
			preparedStatement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return id;
	}
	*/
	
	public static boolean containsQuery(String text, String query) {
		
		boolean contains = false;

		List<String> queryTokens = Arrays.asList(query.toLowerCase().split("\\s+"));
		for (String queryToken : queryTokens) {
			if ( ! queryToken.isEmpty() ) {

				Pattern pattetn = Pattern.compile(".*\\b" + queryToken + "\\b.*", Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
				Matcher matcher = pattetn.matcher(text.toLowerCase());

				if (matcher.matches()) {
					contains = true;
				} else {
					contains = false;
					return contains;
				}
				matcher.reset();

			}
		}

		return contains;
		
	}
	
	public static void updateQueryExecTimes(Connection dbConnection, long queryId) throws SQLException {
		PreparedStatement preparedStatement = null;
		try {
			String sql = "UPDATE query SET num_of_executions=num_of_executions+1 WHERE id=?;";
			//try {
				preparedStatement = dbConnection.prepareStatement(sql);
				preparedStatement.setLong(1, queryId);
				preparedStatement.executeUpdate();
			/*} catch (SQLException e) {
				e.printStackTrace();
			}*/
		} finally {
			if (preparedStatement!=null) {
				preparedStatement.close();
			}
		}
	}
		
}
