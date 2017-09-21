package gov.usdot.cv.discovery.datasink;

import gov.usdot.cv.common.database.mongodb.MongoOptionsBuilder;
import gov.usdot.cv.common.database.mongodb.dao.QueryObjectRegistrationDataDao;
import gov.usdot.cv.common.database.mongodb.geospatial.Coordinates;
import gov.usdot.cv.common.database.mongodb.geospatial.Geometry;
import gov.usdot.cv.common.database.mongodb.geospatial.Point;
import gov.usdot.cv.common.model.BoundingBox;
import gov.usdot.cv.common.util.PropertyLocator;

import java.net.UnknownHostException;
import java.util.Collection;

import org.apache.log4j.Logger;

import com.mongodb.DBObject;

public class DatabaseHelper {

	private final Logger logger = Logger.getLogger(getClass());
	
	public static final String TIMESTAMP_FIELD		= "timestamp";
	public static final int MAX_RECORDS = 400;
	
	private String collectionName;
	private String geoSpatialFieldName;
	
	private QueryObjectRegistrationDataDao objectRegistrationQueryDao;
	
	public DatabaseHelper(String mongoServerHost, int mongoServerPort, 
			String databaseName, boolean autoConnectRetry, int connectTimeoutMs, 
			String collectionName, String geospatialFieldName) throws UnknownHostException {
		
		logger.info("Constructing MongoDB data access object ...");
		MongoOptionsBuilder optionsBuilder = new MongoOptionsBuilder();
		optionsBuilder.setAutoConnectRetry(autoConnectRetry).setConnectTimeoutMs(connectTimeoutMs);
		
		if (mongoServerHost.endsWith("%s")) {
			String domain = PropertyLocator.getString("RTWS_DOMAIN", null);
			mongoServerHost = String.format(mongoServerHost, domain);
		}
		
		this.objectRegistrationQueryDao = QueryObjectRegistrationDataDao.newInstance(
			mongoServerHost, 
			mongoServerPort, 
			optionsBuilder.build(),
			databaseName);
		
		this.collectionName = collectionName;
		this.geoSpatialFieldName = geospatialFieldName;
	}
	
	public Collection<DBObject> query(DiscoverModel discoverModel) throws IllegalArgumentException {
		BoundingBox bb = buildBoundingBox(discoverModel);
		
		Point nwCorner = buildPoint(bb.getNWLat(), bb.getNWLon());
		Point neCorner = buildPoint(bb.getNWLat(), bb.getSELon());
		Point seCorner = buildPoint(bb.getSELat(), bb.getSELon());
		Point swCorner = buildPoint(bb.getSELat(), bb.getNWLon());

		Geometry geometry = buildGeometry(
			Geometry.POLYGON_TYPE, 
			buildCoordinates(
				nwCorner, 
				neCorner, 
				seCorner, 
				swCorner));

		return objectRegistrationQueryDao.findAll(collectionName, discoverModel.serviceId, 
				geoSpatialFieldName, geometry, TIMESTAMP_FIELD, MAX_RECORDS);
	}
	
	private BoundingBox buildBoundingBox(DiscoverModel discoverModel) throws IllegalArgumentException {
		Double nwLat = discoverModel.nwPos.lat;
		Double nwLon = discoverModel.nwPos.lon;
		Double seLat = discoverModel.sePos.lat;
		Double seLon = discoverModel.sePos.lon;
			
		BoundingBox.Builder builder = new BoundingBox.Builder();
		builder.setNWLat(nwLat).setNWLon(nwLon).setSELat(seLat).setSELon(seLon);
		BoundingBox bb = builder.build();
		
		if (!bb.isValid())
			throw new IllegalArgumentException(bb.getValidationError());
		
		return bb;
	}
	
	private Point buildPoint(double lat, double lon) {
		Point.Builder builder = new Point.Builder();
		builder.setLat(lat).setLon(lon);
		return builder.build();
	}
	
	private Coordinates buildCoordinates(
			Point nwCorner, 
			Point neCorner, 
			Point seCorner, 
			Point swCorner) {
		Coordinates.Builder builder = new Coordinates.Builder();
		// Note: MongoDB requires that all geometry shape start and end at the same point
		builder.addPoint(nwCorner).addPoint(neCorner).addPoint(seCorner).addPoint(swCorner).addPoint(nwCorner);
		return builder.build();
	}
	
	private Geometry buildGeometry(String type, Coordinates coordinates) {
		Geometry.Builder builder = new Geometry.Builder();
		builder.setType(type).setCoordinates(coordinates);
		return builder.build();
	}
}
