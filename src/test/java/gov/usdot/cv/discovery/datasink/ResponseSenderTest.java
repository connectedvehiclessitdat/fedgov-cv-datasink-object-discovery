package gov.usdot.cv.discovery.datasink;

import static org.junit.Assert.fail;
import gov.usdot.asn1.generated.j2735.J2735;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import com.oss.asn1.AbstractData;
import com.oss.asn1.Coder;
import com.oss.asn1.ControlTableNotFoundException;
import com.oss.asn1.EncodeFailedException;
import com.oss.asn1.EncodeNotSupportedException;
import com.oss.asn1.InitializationException;

public class ResponseSenderTest {

	@Test
	public void testBuildDiscoveryDataZeroRecords() throws IOException, ControlTableNotFoundException, InitializationException {
		String jsonFile = "src/test/resources/discover_good.json";
		String json = FileUtils.readFileToString(new File(jsonFile));
		DiscoverModel model = DiscoverModel.fromJSON(json);
		model.validate();
		
		ResponseSender rs = new ResponseSender("127.0.0.1", 27000, false);
		AbstractData data = rs.buildDiscoveryData(model, null);
		System.out.println(data);
		
		ByteArrayOutputStream sink = new ByteArrayOutputStream();
		J2735.initialize();
		Coder coder = J2735.getPERUnalignedCoder();
		try {
			coder.encode(data, sink);
		} catch (EncodeFailedException e) {
			fail("Zero service records message failed to encode: " + e);
		} catch (EncodeNotSupportedException e) {
			fail("Zero service records message failed to encode: " + e);
		}
	}
	
}
