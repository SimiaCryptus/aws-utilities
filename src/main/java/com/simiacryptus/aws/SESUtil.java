/*
 * Copyright (c) 2018 by Andrew Charneski.
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
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

public class SESUtil {
  
  public static void send(final AmazonSimpleEmailService ses, final String subject, final String to, final String body, final File... attachments) {
    try {
      ses.sendRawEmail(new SendRawEmailRequest(toRaw(getMessage(
        Session.getDefaultInstance(new Properties()), subject, to,
        mix(Stream.concat(Stream.of(wrap(getEmailBody(body))), Arrays.stream(attachments).map(SESUtil::toAttachment)).toArray(i -> new MimeBodyPart[i]))
      ))));
    } catch (IOException | MessagingException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static MimeMessage getMessage(final Session session, final String subject, final String to, final MimeMultipart content) throws MessagingException {
    MimeMessage message = new MimeMessage(session);
    message.setSubject(subject, "UTF-8");
    message.setFrom(new InternetAddress("acharneski@gmail.com"));
    message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
    message.setContent(content);
    return message;
  }
  
  public static MimeMultipart getEmailBody(final String body) throws MessagingException {
    MimeMultipart multipart = new MimeMultipart("alternative");
    MimeBodyPart textPart = new MimeBodyPart();
    textPart.setContent(body, "text/plain; charset=UTF-8");
    multipart.addBodyPart(textPart);
    return multipart;
  }
  
  @Nonnull
  public static RawMessage toRaw(final MimeMessage message) throws IOException, MessagingException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    message.writeTo(outputStream);
    return new RawMessage(ByteBuffer.wrap(outputStream.toByteArray()));
  }
  
  public static MimeMultipart mix(final MimeBodyPart... parts) throws MessagingException {
    MimeMultipart multipart = new MimeMultipart("mixed");
    for (final MimeBodyPart part : parts) {
      multipart.addBodyPart(part);
    }
    return multipart;
  }
  
  public static MimeBodyPart wrap(final MimeMultipart content) throws MessagingException {
    MimeBodyPart mimeBodyPart = new MimeBodyPart();
    mimeBodyPart.setContent(content);
    return mimeBodyPart;
  }
  
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
}
