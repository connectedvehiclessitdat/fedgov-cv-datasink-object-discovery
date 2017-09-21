package gov.usdot.cv.discovery.datasink;

import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DiscoverModel {

	private static final ObjectMapper mapper;
	
	static {
		mapper = new ObjectMapper();
	}
	
	public static DiscoverModel fromJSON(String json) throws JsonParseException, JsonMappingException, IOException {
		InputStream is = new ByteArrayInputStream(json.getBytes());
		DiscoverModel model = mapper.readValue(is, DiscoverModel.class);
		return model;
	}
	
	public int dialogId;
	public String receiptId;
	public int sequenceId;
	public int groupId;
	public int requestId;
	public int serviceId;
	
	public String destHost;
	public int destPort;
	public String fromForwarder;
	public String certificate;
	
	public Position nwPos;
	public Position sePos;
	
	public static class Position {
		public double lat;
		public double lon;
	}
	
	public void validate() {
		if (dialogId != SemiDialogID.objDisc.longValue()) {
			throw new IllegalArgumentException("Invalid dialogId " + dialogId + " for DiscoveryDataRequest");
		}
		
		if (destHost == null || destPort == 0) {
			throw new IllegalArgumentException("Destination host/port is missing, ignoring data request.");
		}
		
		if (nwPos == null) {
			throw new IllegalArgumentException("Missing northwest position object.");
		} else {
			validateLat("nwPos.lat", nwPos.lat);
			validateLon("nwPos.lon", nwPos.lon);
		}
		
		if (sePos == null) {
			throw new IllegalArgumentException("Missing southeast position object.");
		} else {
			validateLat("sePos.lat", sePos.lat);
			validateLon("sePos.lon", sePos.lon);
		}
	}
	
	private void validateLat(String name, double lat) {
		// 0.0 is invalid for our purposes, catches uninitialized values
		if (lat == 0.0)
			throw new IllegalArgumentException(name + " is required");
		if (lat < -90.0 | lat > 90.0)
			throw new IllegalArgumentException(name + " " + lat + " is not a valid Latitude value");
	}
	
	private void validateLon(String name, double lon) {
		// 0.0 is invalid for our purposes, catches uninitialized values
		if (lon == 0.0)
			throw new IllegalArgumentException(name + " is required");
		if (lon < -180.0 | lon > 180.0)
			throw new IllegalArgumentException(name + " " + lon + " is not a valid Longitude value");
	}

	@Override
	public String toString() {
		return "DiscoverModel [dialogId=" + dialogId + ", receiptId="
				+ receiptId + ", sequenceId=" + sequenceId + ", groupId="
				+ groupId + ", requestId=" + requestId + ", serviceId="
				+ serviceId + ", destHost=" + destHost + ", destPort="
				+ destPort + ", fromForwarder=" + fromForwarder
				+ ", certificate=" + certificate + ", nwPos=" + nwPos
				+ ", sePos=" + sePos + "]";
	}
	
}
