package com.rollingcount;
/**
 * Created by tao on 26/07/15.
 */

import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class DataGenerator {
    private static List<String> getHashTags(String twit)
    {
        ArrayList<String> topics = new ArrayList<String>();

        int startIndex = 0;
        int fromIndex = 0;
        int toIndex = 0;
        while (true) {
            fromIndex = twit.indexOf("HashtagEntityJSONImpl{", startIndex);

            if (fromIndex < 0)
                break;

            startIndex = fromIndex;
            fromIndex = twit.indexOf("text=", startIndex);

            if (fromIndex < 0)
                break;

            fromIndex += 6;
            startIndex = fromIndex;
            toIndex = twit.indexOf("}", startIndex) - 1;

            topics.add(twit.substring(fromIndex, toIndex));

            System.out.println(twit.substring(fromIndex, toIndex));
        }

        return topics;
    }
    public static void main(String[] args) throws Exception {

        // TwitterStreamFactory fact = new TwitterStreamFactory(new ConfigurationBuilder().setUser("andchat1").setPassword("and123456!").build());
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb
                .setOAuthConsumerKey("uAlvehs52GY9lWBDWfETms0Vs")
                .setOAuthConsumerSecret("KYLNKxP2Rq6HPVc49z4qcBvMIJEiKU62JDBLvfuho8XXqiMzuA")
                .setOAuthAccessToken("2847226595-6MYimsk1SK8W1xqEcAI5rOzOmaeQC4RYexBonGy")
                .setOAuthAccessTokenSecret("KO0AXstVC7V4A0PgvWW4cnTzQ9wOA9SIMfnF6PDU747Er");

        TwitterStreamFactory fact = new TwitterStreamFactory(cb.build());
        TwitterStream twitterStream = fact.getInstance();
        String filename = ".";
        final PrintWriter writer = new PrintWriter(new FileOutputStream(new File(filename + "/twitter.data"), true));

        StatusListener listener = new StatusListener() {

            public void onStatus(Status status) {
                for (String s : getHashTags(status.toString())) {
                    writer.println(s);
                    writer.flush();
                }
            }


            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                //System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
            }


            public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
            }


            public void onScrubGeo(long userId, long upToStatusId) {
                System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
            }


            public void onStallWarning(StallWarning warning) {
                System.out.println("Got stall warning:" + warning);
            }


            public void onException(Exception ex) {
                ex.printStackTrace();
            }
        };
        twitterStream.addListener(listener);
        twitterStream.sample();

    }
}