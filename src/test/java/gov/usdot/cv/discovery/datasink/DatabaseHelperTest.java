package gov.usdot.cv.discovery.datasink;

import gov.usdot.cv.discovery.datasink.DatabaseHelper;
import gov.usdot.cv.discovery.datasink.DiscoverModel;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import com.mongodb.DBObject;

public class DatabaseHelperTest {

	public static DatabaseHelper dbHelper;
	
	@BeforeClass
	public static void setup() throws Exception {
		String mongoServerHost = "54.83.140.41";
		int mongoServerPort = 27017;
		String databaseName = "cvdb";
		boolean autoConnectRetry = true;
		int connectTimeoutMs = 0;
		String collectionName = "objectRegisterTest";
		String geospatialFieldName = "region";
		
		dbHelper = new DatabaseHelper(mongoServerHost, mongoServerPort, 
				databaseName, autoConnectRetry, connectTimeoutMs, collectionName, geospatialFieldName);
	}
	
	@Test
	public void testQuery() throws IOException {
		String jsonFile = "src/test/resources/discover_good.json";
		String json = FileUtils.readFileToString(new File(jsonFile));
		DiscoverModel model = DiscoverModel.fromJSON(json);
		model.validate();
		Collection<DBObject> dbObjects = dbHelper.query(model);
		System.out.println(dbObjects);
	}
	
}
