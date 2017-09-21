package gov.usdot.cv.discovery.datasink;

import gov.usdot.cv.discovery.datasink.DiscoverModel;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class DiscoverModelTest {

	@Test
	public void testModelDeserialization() throws IOException {
		String jsonFile = "src/test/resources/discover_good.json";
		String json = FileUtils.readFileToString(new File(jsonFile));
		DiscoverModel model = DiscoverModel.fromJSON(json);
		model.validate();
	}
}
