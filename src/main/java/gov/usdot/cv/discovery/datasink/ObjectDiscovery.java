package gov.usdot.cv.discovery.datasink;

import gov.usdot.asn1.generated.j2735.semi.SemiDialogID;

import java.util.Collection;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.deleidos.rtws.commons.exception.InitializationException;
import com.deleidos.rtws.core.framework.Description;
import com.deleidos.rtws.core.framework.SystemConfigured;
import com.deleidos.rtws.core.framework.UserConfigured;
import com.deleidos.rtws.core.framework.processor.AbstractDataSink;
import com.mongodb.DBObject;

@Description("Processes object discovery requests against the object registration database")
public class ObjectDiscovery extends AbstractDataSink {
	
	private final Logger logger = Logger.getLogger(getClass());
	
	// Mongo
	private String 			mongoServerHost;
	private int    			mongoServerPort;
	private String 			databaseName;
	private boolean 		autoConnectRetry = true;
	private int 			connectTimeoutMs = 0;
	private String			collectionName;
	private String			geospatialFieldName;
	// ResponseSender
	private boolean			forwardAll = false;
	private String 			bundleForwarderHost;
	private int    			bundleForwarderPort = -1;
	// ReceiptSender
	private String			receiptJmsHost;
	private int				receiptJmsPort = -1;
	private String 			topicName;
	
	private DatabaseHelper dbHelper;
	private ResponseSender responseSender;
	private ReceiptSender  receiptSender;
	
	@Override
	@SystemConfigured(value = "Object Discovery DataSink")
	public void setName(String name) {
		super.setName(name);
	}
	
	@Override
	@SystemConfigured(value = "objectdiscovery")
	public void setShortname(String shortname) {
		super.setShortname(shortname);
	}
	
	@UserConfigured(value="cvdb", description="The name of the object registration database")
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	
	public String getDatabaseName() {
		return this.databaseName;
	}
	
	@UserConfigured(
		value= "127.0.0.1", 
		description="The MongoDB server hostname.", 
		flexValidator={"StringValidator minLength=2 maxLength=1024"})
	public void setMongoServerHost(String mongoServerHost) {
		if (mongoServerHost != null) {
			this.mongoServerHost = mongoServerHost.trim();
		}
	}
		
	@NotNull
	public String getMongoServerHost() {
		return this.mongoServerHost;
	}
		
	@UserConfigured(
		value = "27017", 
		description = "The MongoDB server port number.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setMongoServerPort(int mongoServerPort) {
		this.mongoServerPort = mongoServerPort;
	}
		
	@Min(0)
	@Max(65535)
	public int getMongoServerPort() {
		return this.mongoServerPort;
	}
	
	@UserConfigured(
		value = "true",
		description = "MongoDB client auto connect retry flag.",
		flexValidator = {"RegExpValidator expression=true|false"})
	public void setAutoConnectRetry(boolean autoConnectRetry) {
		this.autoConnectRetry = autoConnectRetry;
	}
	
	@NotNull
	public boolean getAutoConnectRetry() {
		return this.autoConnectRetry;
	}
		
	@UserConfigured(
		value = "3000",
		description = "Time (in milliseconds) to wait for a successful connection.",
		flexValidator = {"NumberValidator minValue=0 maxValue=" + Integer.MAX_VALUE})
	public void setConnectTimeoutMs(int connectTimeoutMs) {
		this.connectTimeoutMs = connectTimeoutMs;
	}
	
	@NotNull
	public int getConnectTimeoutMs() {
		return this.connectTimeoutMs;
	}
	
	@UserConfigured(
		value = "true",
		description = "Flag indicating if all responses will be forwarded or not.",
		flexValidator = {"RegExpValidator expression=true|false"})
	public void setForwardAll(boolean forwardAll) {
		this.forwardAll = forwardAll;
	}
	
	@NotNull
	public boolean getForwardAll() {
		return this.forwardAll;
	}
	
	@UserConfigured(
		value= "127.0.0.1", 
		description="The bundle forwarder host.", 
		flexValidator={"StringValidator minLength=2 maxLength=1024"})
	public void setBundleForwarderHost(String bundleForwarderHost) {
		this.bundleForwarderHost = bundleForwarderHost;
	}
			
	@NotNull
	public String getBundleForwarderHost() {
		return this.bundleForwarderHost;
	}
			
	@UserConfigured(
		value = "46761", 
		description = "The bundle forwarder port number.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setBundleForwarderPort(int bundleForwarderPort) {
		this.bundleForwarderPort = bundleForwarderPort;
	}
			
	@Min(0)
	@Max(65535)
	public int getBundleForwarderPort() {
		return this.bundleForwarderPort;
	}
	
	@UserConfigured(
		value= "", 
		description="The receipt jms server hostname.", 
		flexValidator={"StringValidator minLength=0 maxLength=1024"})
	public void setReceiptJmsHost(String receiptJmsHost) {
		this.receiptJmsHost = receiptJmsHost;
	}
	
	@NotNull
	public String getReceiptJmsHost() {
		return this.receiptJmsHost;
	}
	
	@UserConfigured(
		value = "61617", 
		description = "The receipt jms server port.", 
		flexValidator = "NumberValidator minValue=0 maxValue=65535")
	public void setReceiptJmsPort(int receiptJmsPort) {
		this.receiptJmsPort = receiptJmsPort;
	}
	
	@Min(0)
	@Max(65535)
	public int getReceiptJmsPort() {
		return this.receiptJmsPort;
	}
	
	@UserConfigured(
		value = "cv.receipts",
		description = "The jms topic to place receipts.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setReceiptTopicName(String topicName) {
		this.topicName = topicName;
	}
	
	@UserConfigured(
		value = "region",
		description = "Name of the field to perform geospatial query.",
		flexValidator = {"StringValidator minLength=2 maxLength=1024"})
	public void setGeospatialFieldName(String geospatialFieldName) {
		this.geospatialFieldName = geospatialFieldName;
	}
	
	@UserConfigured(value = "objectRegister", description = "Name of the object registration data collection.", 
			flexValidator = { "StringValidator minLength=2 maxLength=1024" })
	public void setCollectionName(String collectionName) {
		this.collectionName = collectionName;
	}
	
	public void initialize() throws InitializationException {
		try {
			dbHelper = new DatabaseHelper(mongoServerHost, mongoServerPort, 
				databaseName, autoConnectRetry, connectTimeoutMs, collectionName, geospatialFieldName);
			receiptSender = new ReceiptSender(receiptJmsHost, receiptJmsPort, topicName);
			responseSender = new ResponseSender(bundleForwarderHost, bundleForwarderPort, forwardAll);
		} catch (Exception ex) {
			throw new InitializationException("Failed to initialize QueryProcessor.", ex);
		}
	}
	
	public void dispose() {
		if (this.receiptSender != null) {
			this.receiptSender.close();
			this.receiptSender = null;
		}
	}

	@Override
	protected void processInternal(JSONObject record, FlushCounter counter) {
		try {
			logger.debug("Processing discovery request: " + record.toString());
			
			long dialogId = record.getInt("dialogId");
			if (dialogId == SemiDialogID.objDisc.longValue()) {
				DiscoverModel discoverModel = DiscoverModel.fromJSON(record.toString());
				discoverModel.validate();
				logger.debug("Running Object Discover Query for " + discoverModel);
				Collection<DBObject> result = dbHelper.query(discoverModel);
				logger.debug("Query results " + result);
				responseSender.sendResponse(discoverModel, result);
				receiptSender.sendReceipt(discoverModel);
			} else {
				logger.error("Received unexpected dialogId: " + dialogId + " expected dialogId:" + SemiDialogID.objDisc.longValue());
			}
			
		} catch (Exception ex) {
			logger.error(String.format("Failed to process discovery request: %s", record.toString()), ex);
		} finally {
			counter.noop();
		}
	}
	
	public void flush() {
		logger.debug(String.format("The method flush() is not used by this class '%s'.", this.getClass().getName()));
	}
}