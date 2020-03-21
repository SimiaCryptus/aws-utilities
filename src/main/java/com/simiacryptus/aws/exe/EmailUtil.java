/*
 * Copyright (c) 2020 by Andrew Charneski.
 *
 * The author licenses this file to you under the
 * Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy
 * of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.simiacryptus.aws.exe;

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder;
import com.simiacryptus.aws.SESUtil;
import com.simiacryptus.notebook.NotebookOutput;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmailUtil {

  private static final Logger logger = LoggerFactory.getLogger(EmailUtil.class);

  public static void sendCompleteEmail(
      final NotebookOutput log,
      final long startTime,
      String emailAddress,
      @Nonnull final Map<File, URL> uploads,
      boolean emailFiles
  ) {
    String fileName = log.getFileName();
    File workingDir = log.getRoot();
    File htmlFile = new File(workingDir, fileName + ".html");
    File zipFile = new File(workingDir, fileName + ".zip");
    File pdfFile = new File(workingDir, fileName + ".pdf");
    String html;
    try {
      html = FileUtils.readFileToString(htmlFile, "UTF-8");
    } catch (IOException e) {
      html = e.getMessage();
    }
    html = editLinks(html, workingDir, uploads);
    html = prepend(html, links(uploads, htmlFile, zipFile, pdfFile));
    SESUtil.send(AmazonSimpleEmailServiceClientBuilder.defaultClient(),
        String.format("%s Completed in %.3fmin", log.getDisplayName(), (System.currentTimeMillis() - startTime) / (1000.0 * 60)),
        emailAddress,
        html,
        html,
        emailFiles ? new File[]{zipFile, pdfFile} : new File[]{});
  }

  @NotNull
  private static String links(@Nonnull Map<File, URL> uploads, File... files) {
    return Arrays.stream(files).map(file -> String.format(
        "<p><a href=\"%s\">%s</a></p>",
        getUrl(file, uploads),
        file.getName())
    ).reduce((a, b) -> a + b).get();
  }

  @NotNull
  private static String prepend(String html, String prepend) {
    String startTag = "<body>";
    if (html.contains(startTag)) {
      return html.replace(startTag, prepend + "<hr/>" + startTag);
    } else {
      return prepend + html;
    }
  }

  @NotNull
  private static String editLinks(String input, File workingDir, @Nonnull Map<File, URL> uploads) {
    Pattern compile = Pattern.compile("\"([^\"]+?)\"");
    Matcher matcher = compile.matcher(input);
    int start = 0;
    String output = "";
    while (matcher.find(start)) {
      output += input.substring(start, matcher.start());
      output += "\"" + resolveLink(workingDir, uploads, matcher.group(1)) + "\"";
      start = matcher.end();
    }
    return output;
  }

  private static String resolveLink(File workingDir, @Nonnull Map<File, URL> uploads, String match) {
    File imageFile = new File(workingDir, match).getAbsoluteFile();
    URL url = uploads.get(imageFile);
    if (null == url) {
      logger.info(String.format("No File Found for %s, reverting to %s", imageFile, match));
    } else {
      logger.info(String.format("Rewriting %s to %s at %s", match, imageFile, url));
      match = url.toString();
    }
    return match;
  }

  private static URL getUrl(File file, @Nonnull Map<File, URL> uploads) {
    File absoluteFile = file.getAbsoluteFile();
    URL url = uploads.get(absoluteFile);
    if (null == url) {
      logger.warn("Not found: " + absoluteFile);
      uploads.forEach((k, v) -> logger.info(k + " uploaded to " + v));
    }
    return url;
  }
}
