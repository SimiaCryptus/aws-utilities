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
import com.amazonaws.services.s3.model.*;
import com.simiacryptus.notebook.NotebookOutput;
import com.simiacryptus.ref.lang.RefAware;
import com.simiacryptus.ref.wrappers.*;
import com.simiacryptus.util.Util;
import com.simiacryptus.util.io.TeeInputStream;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.function.Supplier;

public @RefAware
class S3Util {

  private final static Logger logger = LoggerFactory.getLogger(S3Util.class);

  public static RefMap<File, URL> upload(NotebookOutput log) {
    synchronized (log) {
      return upload(log, AmazonS3ClientBuilder.standard().withRegion(EC2Util.REGION).build());
    }
  }

  public static RefMap<File, URL> upload(NotebookOutput log, AmazonS3 s3) {
    try {
      log.write();
      File root = log.getRoot();
      URI archiveHome = log.getArchiveHome();
      logger.info(String.format("Files in %s to be archived in %s", root.getAbsolutePath(), archiveHome));
      RefHashMap<File, URL> map = new RefHashMap<>();
      if (null != archiveHome) {
        logFiles(root);
        if (null == archiveHome || (archiveHome.getScheme().startsWith("s3") && (null == archiveHome.getHost()
            || archiveHome.getHost().isEmpty() || "null".equals(archiveHome.getHost())))) {
          logger.info(String.format("No archive destination to publish to: %s", archiveHome));
          return map;
        }
        logger.info(String.format("Resolved %s / %s", archiveHome, log.getName()));
        for (File file : root.listFiles()) {
          map.putAll(S3Util.upload(s3, archiveHome, file));
        }
      }
      return map;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void logFiles(File f) {
    if (f.isDirectory()) {
      for (File child : f.listFiles()) {
        logFiles(child);
      }
    } else
      logger.info(String.format("File %s length %s", f.getAbsolutePath(), f.length()));
  }

  public static RefMap<File, URL> upload(final AmazonS3 s3, final URI path, final File file) {
    return upload(s3, path, file, 3);
  }

  public static RefMap<File, URL> upload(final AmazonS3 s3, final URI path, final File file, int retries) {
    try {
      RefHashMap<File, URL> map = new RefHashMap<>();
      if (!file.exists()) {
        if (null != map)
          map.freeRef();
        throw new RuntimeException(file.toString());
      }
      if (null == path) {
        return map;
      }
      String bucket = path.getHost();
      String scheme = path.getScheme();
      if (file.isFile()) {
        String reportPath = path.resolve(file.getName()).getPath().replaceAll("//", "/").replaceAll("^/", "");
        if (scheme.startsWith("s3")) {
          logger.info(String.format("Uploading file %s to s3 %s/%s", file.getAbsolutePath(), bucket, reportPath));
          boolean upload;
          try {
            ObjectMetadata existingMetadata;
            if (s3.doesObjectExist(bucket, reportPath))
              existingMetadata = s3.getObjectMetadata(bucket, reportPath);
            else
              existingMetadata = null;
            if (null != existingMetadata) {
              if (existingMetadata.getContentLength() != file.length()) {
                logger.info(String.format("Removing outdated file %s/%s", bucket, reportPath));
                s3.deleteObject(bucket, reportPath);
                upload = true;
              } else {
                logger.info(String.format("Existing file %s/%s", bucket, reportPath));
                upload = false;
              }
            } else {
              logger.info(String.format("Not found file %s/%s", bucket, reportPath));
              upload = true;
            }
          } catch (AmazonS3Exception e) {
            logger.info(String.format("Error listing %s/%s", bucket, reportPath), e);
            upload = true;
          }
          if (upload) {
            s3.putObject(
                new PutObjectRequest(bucket, reportPath, file).withCannedAcl(CannedAccessControlList.PublicRead));
            map.put(file.getAbsoluteFile(), s3.getUrl(bucket, reportPath));
          }
        } else {
          try {
            logger.info(String.format("Copy file %s to %s", file.getAbsolutePath(), reportPath));
            FileUtils.copyFile(file, new File(reportPath));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        URI filePath = path.resolve(file.getName() + "/");
        if (scheme.startsWith("s3")) {
          String reportPath = filePath.getPath().replaceAll("//", "/").replaceAll("^/", "");
          logger.info(
              String.format("Scanning peer uploads to %s at s3 %s/%s", file.getAbsolutePath(), bucket, reportPath));
          RefCollectors.RefCollector<S3ObjectSummary, ?, RefList<S3ObjectSummary>> temp_07_0001 = RefCollectors
              .toList();
          RefList<S3ObjectSummary> preexistingFiles = s3
              .listObjects(new ListObjectsRequest().withBucketName(bucket).withPrefix(reportPath)).getObjectSummaries()
              .stream().collect(temp_07_0001);
          if (null != temp_07_0001)
            temp_07_0001.freeRef();
          for (S3ObjectSummary preexistingFile : preexistingFiles) {
            logger.info(String.format("Preexisting File: '%s' + '%s'", reportPath, preexistingFile.getKey()));
            map.put(new File(file, preexistingFile.getKey()).getAbsoluteFile(),
                s3.getUrl(bucket, reportPath + preexistingFile.getKey()));
          }
          if (null != preexistingFiles)
            preexistingFiles.freeRef();
        }
        logger.info(String.format("Uploading folder %s to %s", file.getAbsolutePath(), filePath.toString()));
        for (File subfile : file.listFiles()) {
          map.putAll(upload(s3, filePath, subfile));
        }
      }
      return map;
    } catch (Throwable e) {
      if (retries > 0) {
        return upload(s3, path, file, retries - 1);
      }
      throw new RuntimeException("Error uploading " + file + " to " + path, e);
    }
  }

  public static InputStream cache(final AmazonS3 s3, final URI cacheLocation, URI resourceLocation) {
    return cache(s3, cacheLocation, tempFile(cacheLocation), Util.getStreamSupplier(resourceLocation));
  }

  public static InputStream cache(AmazonS3 s3, URI s3File, File localFile, Supplier<InputStream> streamSupplier) {
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
        throw new RuntimeException(e);
      }
    }
  }

  @NotNull
  public static File tempFile(URI file) {
    File localFile;
    try {
      localFile = File.createTempFile(file.getPath(), "data");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return localFile;
  }

  @Nonnull
  public static String defaultPolicy(final String... bucket) {
    String bucketGrant = RefArrays.stream(bucket).map(b -> "{\n" + "      \"Action\": \"s3:*\",\n"
        + "      \"Effect\": \"Allow\",\n" + "      \"Resource\": \"arn:aws:s3:::" + b + "*\"\n" + "    }")
        .reduce((a, b) -> a + "," + b).get();
    return "{\n" + "  \"Version\": \"2012-10-17\",\n" + "  \"Statement\": [\n" + "    " + bucketGrant + ",\n"
        + "    {\n" + "      \"Action\": \"s3:ListBucket*\",\n" + "      \"Effect\": \"Allow\",\n"
        + "      \"Resource\": \"arn:aws:s3:::*\"\n" + "    },\n" + "    {\n"
        + "      \"Action\": [\"ses:SendEmail\",\"ses:SendRawEmail\"],\n" + "      \"Effect\": \"Allow\",\n"
        + "      \"Resource\": \"*\"\n" + "    }\n" + "  ]\n" + "}";
  }
}
