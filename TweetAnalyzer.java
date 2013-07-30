import java.util.Iterator;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.UserMentionEntity;
import twitter4j.auth.AccessToken;
import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

/**
 * @author Asadullah
 * This class extracts 100 tweets, store them in MongodDB and returns the top five user Id mentioned,
 * top five retweeted tweets and the sources for the tweets
 *
 */
public class TweetAnalyzer {

	public static Twitter twitter;
	public static Mongo mongo;

	public static void main(String args[])throws Exception {

		twitter = new TwitterFactory().getInstance();
		if (args.length < 5){
			System.err.println("Usage: <consumer Key>  <consumerSecretKey> <token> <tokenSecret> <query string>");
			System.exit(1);
		}
		twitter.setOAuthConsumer(args[0], args[1]);
		twitter.setOAuthAccessToken(new AccessToken(args[2],args[3]));
		mongo = new Mongo();
		DB db = mongo.getDB("test");
		DBCollection tweetinfo = db.getCollection("tweets");
		DBCollection mentionsinfo = db.getCollection("mentions");

		// Writing Tweets to MongoDB
		writetweetstoMongo(tweetinfo,mentionsinfo,args[4]);

		getTopReTweets(tweetinfo);

		//Retrieving Top 5 Mentioned User Ids
		getTopMentioned(mentionsinfo);

		// Get Tweets source info
		getTweetsSourceInfo(tweetinfo);

		mongo.close();
	}

	/**
	 * Inserts tweets data into the database
	 * @param tweetinfo
	 * @param mentionsinfo
	 * @param queryString
	 */
	private static void writetweetstoMongo(DBCollection tweetinfo,DBCollection mentionsinfo,String queryString) {
		try {
			Query query = new Query(queryString);
			query.setCount(100);
			query.setSince("2013-01-01");
			QueryResult result = twitter.search(query);
			System.out.println("Total Tweets Fetched : " + result.getTweets().size()) ;

			for (twitter4j.Status tweet : result.getTweets()) {
				long id = tweet.getId();

				BasicDBObject info = new BasicDBObject();
				info.put("id", id);
				info.put("retweet", tweet.getRetweetCount());
				info.put("text", tweet.getText());
				info.put("userid", tweet.getUser().getId());
				info.put("retweet", tweet.getRetweetCount());
				info.put("isfavorited", tweet.isFavorited());

				String source  = tweet.getSource();

				if (source!= null){
					// If source has a hyper link, then get a substring
					if (source.contains("</a>")){
						source = source.substring(source.indexOf(">")+1,source.indexOf("</a>"));
					}
					// add the source
					info.put("source", source);
				}

				UserMentionEntity[] mentionArray = tweet.getUserMentionEntities();
				for (UserMentionEntity user : mentionArray){
					BasicDBObject minfo = new BasicDBObject();
					minfo.put("userid",String.valueOf(user.getId()));
					minfo.put("tweetid", id);
					mentionsinfo.insert(minfo);
				}

				if (tweet.getPlace() != null){
					info.put("country", tweet.getPlace().getCountry());
				}
				tweetinfo.insert(info);              
			}

		} catch (TwitterException te) {
			te.printStackTrace();
			System.exit(-1);
		}
	}

	/**
	 * Retrieves the top five re-tweets
	 * @param coll 
	 */
	public static void getTopReTweets(DBCollection coll){
		System.out.println("------------Retrieving top five retweets ------------\n");
		BasicDBObject query = new BasicDBObject();
		DBCursor cursor = coll.find(query).sort(new BasicDBObject("retweet",-1)).limit(5); 
		try {
			while(cursor.hasNext()) {
				DBObject entry = cursor.next();
				System.out.println("Tweet No: " + entry.get("id") + " has been retweeted :" + entry.get("retweet") + " times.");
			}
		} finally {
			cursor.close();
		}
		System.out.println("------------End of Retrieving top five retweets ------------\n");
	}

	/**
	 * Retrieves the top five mentioned users
	 * @param tweetinfo
	 */
	public static void getTopMentioned(DBCollection tweetinfo){
		System.out.println("------------Retrieving top five user mentioned ------------\n");
		AggregationOutput output = getAggregationOutput(tweetinfo,"userid","$userid");
		Iterator<DBObject> iterator = output.results().iterator();

		int top_five = 0;
		while (iterator.hasNext() && top_five < 5){
			DBObject entry =  iterator.next();
			System.out.println("User ID: " + entry.get("_id") + " has been mentioned " + entry.get("total") + " times");
			top_five++;
		}
		System.out.println("------------ End of Retrieving top five user mentioned ------------\n");
	}

	/**
	 * Retrieves the top five tweet sources
	 * @param tweetinfo
	 */
	private static void getTweetsSourceInfo(DBCollection tweetinfo) {
		System.out.println("------------Retrieving tweet source info ------------\n");

		AggregationOutput output = getAggregationOutput(tweetinfo,"source","$source");
		Iterator<DBObject> iterator = output.results().iterator();

		int top_five = 0;
		while (iterator.hasNext() && top_five < 5){
			DBObject entry =  iterator.next();
			System.out.println(entry.get("_id") + " has been the source for " + entry.get("total") + " tweetes");
			top_five ++;
		}
		System.out.println("------------End of Retrieving tweet source info ------------\n");
	}

	/**
	 * A helper for aggregations
	 * @param tweetinfo
	 * @param field
	 * @param groupfield
	 * @return AggregationOutput
	 */
	private static AggregationOutput getAggregationOutput(DBCollection tweetinfo,String field,String groupfield) {
		BasicDBObject match = new BasicDBObject();
		match.put("$match", new BasicDBObject());
		DBObject fields = new BasicDBObject();
		fields.put(field,1);

		DBObject groupFields = new BasicDBObject( "_id", groupfield);
		groupFields.put("total", new BasicDBObject( "$sum", 1));

		DBObject sortFields = new BasicDBObject( "total", -1);

		DBObject project = new BasicDBObject();
		project.put("$project", fields);
		DBObject group = new BasicDBObject("$group", groupFields);
		DBObject sort = new BasicDBObject("$sort", sortFields);
		AggregationOutput output =  tweetinfo.aggregate(match,project,group,sort);
		return output;
	}

}