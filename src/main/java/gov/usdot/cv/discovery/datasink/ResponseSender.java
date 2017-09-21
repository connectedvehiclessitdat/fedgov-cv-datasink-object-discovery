package gov.usdot.cv.discovery.datasink;

import gov.usdot.asn1.generated.j2735.J2735;
import gov.usdot.asn1.generated.j2735.dsrc.Count;
import gov.usdot.asn1.generated.j2735.dsrc.Latitude;
import gov.usdot.asn1.generated.j2735.dsrc.Longitude;
import gov.usdot.asn1.generated.j2735.dsrc.Position3D;
import gov.usdot.asn1.generated.j2735.semi.ConnectionPoint;
import gov.usdot.asn1.generated.j2735.semi.GeoRegion;
import gov.usdot.asn1.generated.j2735.semi.ObjectDiscoveryData;
import gov.usdot.asn1.generated.j2735.semi.ObjectDiscoveryData.ServiceInfo;
import gov.usdot.asn1.generated.j2735.semi.ObjectDiscoveryData.ServiceInfo.ServiceRecords;
import gov.usdot.asn1.generated.j2735.semi.ObjectRegistrationData;
import gov.usdot.asn1.generated.j2735.semi.PortNumber;
import gov.usdot.asn1.generated.j2735.semi.Psid;
import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;
import gov.usdot.asn1.generated.j2735.semi.SemiSequenceID;
import gov.usdot.asn1.generated.j2735.semi.ServiceProviderID;
import gov.usdot.asn1.generated.j2735.semi.ServiceRecord;
import gov.usdot.asn1.generated.j2735.semi.ServiceRecord.ConnectionPoints;
import gov.usdot.asn1.generated.j2735.semi.ServiceRecord.SvcPSIDs;
import gov.usdot.asn1.j2735.J2735Util;
import gov.usdot.cv.common.asn1.GroupIDHelper;
import gov.usdot.cv.common.asn1.TemporaryIDHelper;
import gov.usdot.cv.common.inet.InetPacketSender;
import gov.usdot.cv.common.inet.InetPoint;
import gov.usdot.cv.security.SecurityHelper;
import gov.usdot.cv.security.crypto.CryptoProvider;

import java.io.ByteArrayOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.oss.asn1.AbstractData;
import com.oss.asn1.Coder;
import com.oss.asn1.ControlTableNotFoundException;
import com.oss.asn1.DecodeFailedException;
import com.oss.asn1.DecodeNotSupportedException;
import com.oss.asn1.InitializationException;

public class ResponseSender {

	private static final Logger logger = Logger.getLogger(ResponseSender.class);
	
	private static final String ENCODED_MSG_KEY = "encodedMsg";
	private CryptoProvider cryptoProvider = new CryptoProvider();
	private Coder coder;
	private InetPacketSender dataBundleSender;
	
	public ResponseSender (String bundleForwarderHost, int bundleForwarderPort, boolean forwardAll) 
			throws ControlTableNotFoundException, InitializationException {
		
		J2735.initialize();
		coder = J2735.getPERUnalignedCoder();
		SecurityHelper.initSecurity();
		
		InetPoint forwarderPoint = null;
		if (bundleForwarderHost != null && bundleForwarderPort != 0) {
			try {
				forwarderPoint = new InetPoint(InetAddress.getByName(bundleForwarderHost).getAddress(),
						bundleForwarderPort);
			} catch (UnknownHostException e) {
				logger.error("Error creating forwarder InetPoint ", e);
			}
		}
		
		logger.info(String.format("Forwarder host '%s' and port '%s'.", bundleForwarderHost, bundleForwarderPort));
		logger.info(String.format("Packet sender forwarding all response: %s", forwardAll));
		
		logger.info("Constructing result processors ...");
		dataBundleSender = new InetPacketSender(forwarderPoint);
		dataBundleSender.setForwardAll(forwardAll);
	}
	
	public void sendResponse(DiscoverModel discoverModel, Collection<DBObject> result) throws Exception {
		List<ObjectRegistrationData> records = extractRegistrationRecords(result);
		ObjectDiscoveryData discoveryData = buildDiscoveryData(discoverModel, records);
		sendDiscoveryData(discoverModel, discoveryData);
	}
	
	private List<ObjectRegistrationData> extractRegistrationRecords(Collection<DBObject> result) throws DecodeFailedException, DecodeNotSupportedException {
		List<ObjectRegistrationData> records = new ArrayList<ObjectRegistrationData>();
		Iterator<DBObject> it = result.iterator();
		while(it.hasNext()) {
			DBObject dbObj = it.next();
			if (dbObj instanceof BasicDBObject) {
				BasicDBObject objectRegistrationDataObj = (BasicDBObject) dbObj;
				if (objectRegistrationDataObj.containsField(ENCODED_MSG_KEY)) {
					byte [] message = Base64.decodeBase64(objectRegistrationDataObj.getString(ENCODED_MSG_KEY));
					AbstractData berEncoded = J2735Util.decode(coder, message);
					if (berEncoded instanceof ObjectRegistrationData) {
						records.add((ObjectRegistrationData)berEncoded);
					} else {
						logger.debug(String.format("Encoded message is not of type ObjectRegistrationData: %s", berEncoded.toString()));
						continue;
					}
				}
			}
		}
		return records;
	}
	
	protected ObjectDiscoveryData buildDiscoveryData(
			DiscoverModel discoverModel, 
			List<ObjectRegistrationData> registrationRecords) {
		
		ObjectDiscoveryData discoveryData = new ObjectDiscoveryData();
		discoveryData.setDialogID(SemiDialogID.objDisc);
		discoveryData.setSeqID(SemiSequenceID.data);
		discoveryData.setRequestID(TemporaryIDHelper.toTemporaryID(discoverModel.requestId));
		discoveryData.setGroupID(GroupIDHelper.toGroupID(discoverModel.groupId));
		
		if (registrationRecords == null || registrationRecords.size() == 0) {
			logger.debug("0 ObjectRegistrationData records where found from the database.");
			ServiceInfo serviceInfo = new ServiceInfo();
			// The ASN1 spec does not allow for 0 service records, so setting count to 0 and using "blank" record
			serviceInfo.setCountRecords(new Count(0));
			ServiceRecords serviceRecords = new ServiceRecords();
			serviceRecords.addElement(buildBlankServiceRecord());
			serviceInfo.setServiceRecords(serviceRecords);
			discoveryData.setServiceInfo(serviceInfo);
		} else {
			logger.debug(registrationRecords.size() + " ObjectRegistrationData records where found from the database.");
			
			ServiceInfo serviceInfo = new ServiceInfo();
			// The ASN1 spec maxes out at 10 records, if we have more than 10 we just send the first 10
			int count = registrationRecords.size() > 10 ? 10 : registrationRecords.size();
			serviceInfo.setCountRecords(new Count(count));
			ServiceRecords serviceRecords = new ServiceRecords();
			for (int i=0; i<count; i++) {
				serviceRecords.addElement(registrationRecords.get(i).getServiceRecord());
			}
			serviceInfo.setServiceRecords(serviceRecords);
			discoveryData.setServiceInfo(serviceInfo);
		}
		return discoveryData;
	}
	
	private ServiceRecord buildBlankServiceRecord() {
		ServiceRecord serviceRecord = new ServiceRecord();
		ConnectionPoint cp = new ConnectionPoint(new PortNumber(0));
		ConnectionPoints cps = new ConnectionPoints();
		cps.add(cp);
		serviceRecord.setConnectionPoints(cps);
		
		serviceRecord.setSvcProvider(new ServiceProviderID(ByteBuffer.allocate(4).putInt(0).array()));
		Psid psidObj = new Psid(ByteBuffer.allocate(4).putInt(0).array());
		SvcPSIDs psids = new SvcPSIDs();
		psids.add(psidObj);
		serviceRecord.setSvcPSIDs(psids);
		
		Position3D pos = new Position3D(new Latitude(-900000000), new Longitude(-1800000000));
		GeoRegion geoRegion = new GeoRegion(pos, pos);
		serviceRecord.setServiceRegion(geoRegion);
		
		return serviceRecord;
	}
	
	private void sendDiscoveryData(
			DiscoverModel discoverModel,
			ObjectDiscoveryData discoveryData) throws Exception {
		
		ByteArrayOutputStream sink = new ByteArrayOutputStream();
		coder.encode(discoveryData, sink);
		byte [] payload = sink.toByteArray();
		
		byte[] certificate = discoverModel.certificate != null ? Base64.decodeBase64(discoverModel.certificate): null;
		if (certificate != null) {
			try {
				byte[] certID8 = SecurityHelper.registerCert(certificate, cryptoProvider);
				payload = SecurityHelper.encrypt(payload, certID8, cryptoProvider, SecurityHelper.DEFAULT_PSID);
			} catch (Exception ex) {
				logger.error("Couldn't encrypt outgoing message. Reason: " + ex.getMessage(), ex);
			}
		}
		
		int retries = 3;
		Exception lastEx = null;
		boolean sent = false;
		while (retries > 0) {
			try {
				logger.debug(String.format("Sending Object Discovery data for requestID %s", 
						TemporaryIDHelper.fromTemporaryID(discoveryData.getRequestID())));
				InetPoint destPoint = new InetPoint(discoverModel.destHost, 
						discoverModel.destPort, dataBundleSender.isForwardAll());
				dataBundleSender.forward(destPoint, payload, Boolean.valueOf(discoverModel.fromForwarder));
				sent = true;
				break;
			} catch (Exception ex) {
				logger.error(String.format("Failed to send Object Discovery data for requestID %s", 
						TemporaryIDHelper.fromTemporaryID(discoveryData.getRequestID())));
				lastEx = ex;
			} finally {
				retries--;
			}
			
			try { Thread.sleep(10); } catch (InterruptedException ignore) {}
		}
		
		if (! sent && lastEx != null) throw lastEx;
		
		try { Thread.sleep(10); } catch (InterruptedException ignore) {}
	}
}