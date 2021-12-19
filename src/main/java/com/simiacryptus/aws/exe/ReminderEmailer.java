package com.simiacryptus.aws.exe;

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.simiacryptus.aws.EC2Util;
import com.simiacryptus.aws.SESUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ReminderEmailer {
    private static final Logger logger = LoggerFactory.getLogger(ReminderEmailer.class);

    private static final ScheduledThreadPoolExecutor SCHEDULED_THREAD_POOL_EXECUTOR = new ScheduledThreadPoolExecutor(1);

    public static void main(String[] args) {
        startReminderEmails(args.length > 0? args[0] : "?");
    }

    public static void startReminderEmails(String testName) {
      String emailAddress = UserSettings.load().getEmailAddress();
      SCHEDULED_THREAD_POOL_EXECUTOR.scheduleAtFixedRate(
              () -> sendReminderEmail(testName, emailAddress),
              EC2NodeSettings.reminderTimeMinutes,
              EC2NodeSettings.reminderTimeMinutes,
              TimeUnit.MINUTES);
    }

    private static void sendReminderEmail(final String testName, String emailAddress) {
      try {
        logger.info("Sending uptime reminder email to "+emailAddress+"...");
        String publicHostname = "???";
        String instanceId = "???";
        try {
          publicHostname = EC2Util.getPublicHostname();
          instanceId = EC2Util.getInstanceId();
        } catch (IOException | URISyntaxException e) {
          logger.warn("Error getting EC2 Metadata", e);
        }
        String html = String.format("<html><body>" +
                "<p><a href=\"http://%s:1080/\">The %s report can be monitored at %s</a></p><hr/>" +
                "<p><a href=\"https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#Instances:search=%s\">View instance %s on AWS Console</a></p>" +
                "</body></html>", publicHostname, testName, publicHostname, instanceId, instanceId);
        String txtBody = "Process still running at " + new Date();
        String subject = testName + " Running";
        SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient(), subject, emailAddress, txtBody, html);
      } catch (Throwable e) {
        logger.warn("Error sending reminder email", e);
      }
    }
}
