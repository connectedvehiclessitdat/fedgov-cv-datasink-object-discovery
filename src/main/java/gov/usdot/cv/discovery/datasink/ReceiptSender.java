package gov.usdot.cv.discovery.datasink;

import gov.usdot.cv.common.dialog.Receipt;
import gov.usdot.cv.common.util.PropertyLocator;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.deleidos.rtws.commons.exception.InitializationException;

public class ReceiptSender {

	private final Logger logger = Logger.getLogger(getClass());
	
	private gov.usdot.cv.common.dialog.ReceiptSender rSender;
	
	public ReceiptSender(String receiptJmsHost, int receiptJmsPort, String topicName) {
		
		logger.info("Constructing receipt sender ...");
		String brokerUrl = null;
		if (! StringUtils.isEmpty(receiptJmsHost) && receiptJmsPort != -1) {
			StringBuilder sb = new StringBuilder();
			sb.append("nio://").append(receiptJmsHost).append(':').append(receiptJmsPort);
			brokerUrl = sb.toString();
		} else {
			brokerUrl = PropertyLocator.getString("messaging.external.connection.url");
		}
		
		if (brokerUrl == null) {
			throw new InitializationException("Missing property 'messaging.external.connection.url'.");
		}
	
		String username = PropertyLocator.getString("messaging.external.connection.user");
		if (username == null) {
			throw new InitializationException("Missing property 'messaging.external.connection.user'.");
		}
	
		String password = PropertyLocator.getString("messaging.external.connection.password");
		if (password == null) {
			throw new InitializationException("Missing property 'messaging.external.connection.password'.");
		}
	
		gov.usdot.cv.common.dialog.ReceiptSender.Builder receiptSenderBuilder = 
				new gov.usdot.cv.common.dialog.ReceiptSender.Builder();
		receiptSenderBuilder.setBrokerUrl(brokerUrl).setUsername(username)
			.setPassword(password).setTopicName(topicName);
		
		this.rSender = receiptSenderBuilder.build();
	}
	
	public void sendReceipt(DiscoverModel model) {
		try {
			String receiptId = model.receiptId;
			if (receiptId != null) {
				Receipt.Builder builder = new Receipt.Builder();
				builder.setReceiptId(receiptId);
				
				logger.info(String.format("Sent receipt '%s' for '%s'.", model.receiptId, model.destHost));
				this.rSender.send(builder.build().toString());
			} else {
				logger.warn("Receipt not sent because 'receiptId' field doesn't exist. Record: " + model.toString());
			}
		} catch (Exception ex) {
			logger.error("Failed to send receipt to external jms server.", ex);
		}
	}
	
	public void close() {
		this.rSender.close();
	}
}
