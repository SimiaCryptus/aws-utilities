/*
 * Copyright (c) 2019 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.aws;

import com.amazonaws.services.simpleemail.AmazonSimpleEmailService;
import com.amazonaws.services.simpleemail.model.RawMessage;
import com.amazonaws.services.simpleemail.model.SendRawEmailRequest;
import com.amazonaws.services.simpleemail.model.VerifyEmailAddressRequest;
import com.simiacryptus.ref.wrappers.RefArrays;
import com.simiacryptus.ref.wrappers.RefStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.annotation.Nonnull;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

public class SESUtil {
  private static final Logger logger = LoggerFactory.getLogger(SESUtil.class);

  public static void send(@Nonnull final AmazonSimpleEmailService ses, final String subject, @Nonnull final String to, final String body,
                          @Nonnull final String html, @Nonnull final File... attachments) {
    try {
      RefStream<MimeBodyPart> attachmentStream = RefArrays.stream(attachments)
          .filter(x -> x.exists() && x.length() < 1024 * 1024 * 4).map(attachment -> toAttachment(attachment));
      ses.sendRawEmail(new SendRawEmailRequest(toRaw(getMessage(Session.getDefaultInstance(new Properties()), subject,
          to, mix(RefStream.concat(RefStream.of(wrap(getEmailBody(body, html))), attachmentStream)
              .toArray(i -> new MimeBodyPart[i]))))));
    } catch (@Nonnull IOException | MessagingException e) {
      throw new RuntimeException(e);
    }
  }

  @Nonnull
  public static MimeMessage getMessage(final Session session, final String subject, @Nonnull final String to,
                                       @Nonnull final MimeMultipart content) throws MessagingException {
    MimeMessage message = new MimeMessage(session);
    message.setSubject(subject, "UTF-8");
    message.setFrom(new InternetAddress("acharneski@gmail.com"));
    message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
    message.setContent(content);
    return message;
  }

  @Nonnull
  public static MimeMultipart getEmailBody(final String body, @Nonnull final String html) throws MessagingException {
    MimeMultipart multipart = new MimeMultipart("alternative");
    MimeBodyPart textPart = new MimeBodyPart();
    textPart.setContent(body, "text/plain; charset=UTF-8");
    multipart.addBodyPart(textPart);
    if (!html.isEmpty()) {
      MimeBodyPart htmlPart = new MimeBodyPart();
      htmlPart.setContent(html, "text/html; charset=UTF-8");
      multipart.addBodyPart(htmlPart);
    }
    return multipart;
  }

  @Nonnull
  public static RawMessage toRaw(@Nonnull final MimeMessage message) throws IOException, MessagingException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    message.writeTo(outputStream);
    return new RawMessage(ByteBuffer.wrap(outputStream.toByteArray()));
  }

  @Nonnull
  public static MimeMultipart mix(@Nonnull final MimeBodyPart... parts) throws MessagingException {
    MimeMultipart multipart = new MimeMultipart("mixed");
    for (final MimeBodyPart part : parts) {
      multipart.addBodyPart(part);
    }
    return multipart;
  }

  @Nonnull
  public static MimeBodyPart wrap(@Nonnull final MimeMultipart content) throws MessagingException {
    MimeBodyPart mimeBodyPart = new MimeBodyPart();
    mimeBodyPart.setContent(content);
    return mimeBodyPart;
  }

  @Nonnull
  public static MimeBodyPart toAttachment(final File attachment) {
    MimeBodyPart att = new MimeBodyPart();
    DataSource fds = new FileDataSource(attachment);
    try {
      att.setDataHandler(new DataHandler(fds));
      att.setFileName(fds.getName());
    } catch (MessagingException e) {
      throw new RuntimeException(e);
    }
    return att;
  }

  public static void setup(@Nonnull final AmazonSimpleEmailService ses, final String emailAddress) {
    try {
      List<String> verifiedEmailAddresses = ses.listVerifiedEmailAddresses().getVerifiedEmailAddresses();
      if (verifiedEmailAddresses.contains(emailAddress))
        return;
      ses.verifyEmailAddress(new VerifyEmailAddressRequest().withEmailAddress(emailAddress));
    } catch (Throwable e) {
      logger.warn("Error verifying " + emailAddress, e);
    }
  }
}
