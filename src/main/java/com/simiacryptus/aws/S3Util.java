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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.simiacryptus.notebook.NotebookOutput;
import com.simiacryptus.ref.lang.RefUtil;
import com.simiacryptus.ref.wrappers.RefArrays;
import com.simiacryptus.ref.wrappers.RefString;
import com.simiacryptus.util.S3Uploader;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.io.TeeInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3Util {

  private final static Logger logger = LoggerFactory.getLogger(S3Util.class);

  public static List<String> listFiles(URI location, Predicate<S3ObjectSummary> filter) {
    assert location.getScheme().equals("s3");
    String bucket = location.getHost();
    String path = location.getPath();
    while (path.startsWith("/")) path = path.substring(1);
    return listFiles(bucket, path, filter);
  }

  public static List<String> listFiles(String bucket, String path, Predicate<S3ObjectSummary> filter) {
    AmazonS3 s3 = S3Uploader.buildClientForRegion(S3Uploader.getRegion(bucket));
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(bucket)
        .withPrefix(path)
        .withMaxKeys(Integer.MAX_VALUE);
    return getAllObjectSummaries(s3, s3.listObjects(listObjectsRequest))
        .filter(filter)
        .map(x -> x.getKey())
        .collect(Collectors.toList());
  }

  @Nonnull
  public static Map<File, URL> upload(@Nonnull NotebookOutput log) {
    URI archiveHome = log.getArchiveHome();
    if (archiveHome != null && !archiveHome.getHost().isEmpty()) {
      synchronized (log) {
        return upload(log, AmazonS3ClientBuilder.standard().withRegion(EC2Util.REGION).build());
      }
    } else {
      return new HashMap<>();
    }
  }

  @Nonnull
  public static Map<File, URL> upload(@Nonnull NotebookOutput log, @Nonnull AmazonS3 s3) {
    try {
      log.write();
      File root = log.getRoot();
      URI archiveHome = log.getArchiveHome();
      logger.info(RefString.format("Files in %s to be archived in %s", root.getAbsolutePath(), archiveHome));
      HashMap<File, URL> map = new HashMap<>();
      if (null != archiveHome) {
        logFiles(root);
        if (archiveHome.getScheme().startsWith("s3") && (null == archiveHome.getHost() || archiveHome.getHost().isEmpty() || "null".equals(archiveHome.getHost()))) {
          logger.info(RefString.format("No archive destination to publish to: %s", archiveHome));
          return map;
        }
        logger.info(RefString.format("Uploading %s to %s", log.getFileName(), archiveHome));
        for (File file : root.listFiles()) {
          map.putAll(S3Uploader.upload(s3, archiveHome, file));
        }
      }
      return map;
    } catch (IOException e) {
      throw Util.throwException(e);
    }
  }

  public static void logFiles(@Nonnull File f) {
    if (f.isDirectory()) {
      for (File child : f.listFiles()) {
        logFiles(child);
      }
    } else
      logger.info(RefString.format("File %s length %s", f.getAbsolutePath(), f.length()));
  }

  public static InputStream cache(@Nonnull final AmazonS3 s3, @Nonnull final URI cacheLocation, @Nonnull URI resourceLocation) {
    return cache(s3, cacheLocation, tempFile(cacheLocation), Util.getStreamSupplier(resourceLocation));
  }

  public static InputStream cache(@Nonnull AmazonS3 s3, @Nonnull URI s3File, @Nonnull File localFile, @Nonnull Supplier<InputStream> streamSupplier) {
    final String path = s3File.getPath().replaceAll("^/", "");
    if (s3.doesObjectExist(s3File.getHost(), path)) {
      return s3.getObject(s3File.getHost(), path).getObjectContent();
    } else {
      try {
        localFile.deleteOnExit();
        return new TeeInputStream(streamSupplier.get(), new FileOutputStream(localFile)) {
          @Override
          public void close() throws IOException {
            super.close();
            s3.putObject(s3File.getHost(), path, localFile);
          }
        };
      } catch (IOException e) {
        throw Util.throwException(e);
      }
    }
  }

  @Nonnull
  public static File tempFile(@Nonnull URI file) {
    File localFile;
    try {
      localFile = File.createTempFile(file.getPath(), "data");
    } catch (IOException e) {
      throw Util.throwException(e);
    }
    return localFile;
  }

  @Nonnull
  public static String defaultPolicy(@Nonnull final String... bucket) {
    String bucketGrant = RefUtil.get(RefArrays.stream(bucket).map(b -> "{\n" + "      \"Action\": \"s3:*\",\n"
        + "      \"Effect\": \"Allow\",\n" + "      \"Resource\": \"arn:aws:s3:::" + b + "*\"\n" + "    }")
        .reduce((a, b) -> a + "," + b));
    return "{\n" + "  \"Version\": \"2012-10-17\",\n" + "  \"Statement\": [\n" + "    " + bucketGrant + ",\n"
        + "    {\n" + "      \"Action\": \"s3:ListBucket*\",\n" + "      \"Effect\": \"Allow\",\n"
        + "      \"Resource\": \"arn:aws:s3:::*\"\n" + "    },\n" + "    {\n"
        + "      \"Action\": [\"ses:SendEmail\",\"ses:SendRawEmail\"],\n" + "      \"Effect\": \"Allow\",\n"
        + "      \"Resource\": \"*\"\n" + "    }\n" + "  ]\n" + "}";
  }

  private static Stream<S3ObjectSummary> getAllObjectSummaries(AmazonS3 s3, ObjectListing listing) {
    if (listing.isTruncated()) {
      return Stream.concat(
          listing.getObjectSummaries().stream(),
          getAllObjectSummaries(s3, s3.listNextBatchOfObjects(listing))
      );
    } else {
      return listing.getObjectSummaries().stream();
    }
  }

}
