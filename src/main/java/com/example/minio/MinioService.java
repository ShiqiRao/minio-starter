package com.example.minio;

import io.minio.*;
import io.minio.errors.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

@Service
public class MinioService implements ApplicationRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MinioConfiguration.class);

    @Autowired
    private MinioClient minioClient;

    @Autowired
    private MinioConfigurationProperties minioConfigurationProperties;

    /**
     * 程序启动时自动创建bucket
     */
    public void run(ApplicationArguments args) throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(minioConfigurationProperties.getBucket()).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(minioConfigurationProperties.getBucket()).build());
            LOGGER.info("{} is created successfully", minioConfigurationProperties.getBucket());
        }
    }

    public void removeBucket() throws Exception {
        //移除存储桶
        boolean found = minioClient.bucketExists(BucketExistsArgs
                .builder()
                .bucket(minioConfigurationProperties.getBucket())
                .build());
        if (found) {
            minioClient.removeBucket(RemoveBucketArgs
                    .builder()
                    .bucket(minioConfigurationProperties.getBucket())
                    .build());
            LOGGER.info("{} removed successfully", minioConfigurationProperties.getBucket());
        } else {
            LOGGER.info("{} does not exist", minioConfigurationProperties.getBucket());
        }
    }

    public ObjectWriteResponse uploadObject(String objectName, InputStream inputStream) throws Exception {
        //以输入流形式上传文件
        return minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(minioConfigurationProperties.getBucket())
                        .object(objectName)
                        .stream(inputStream, inputStream.available(), -1)
                        .build());
    }

    public void removeObject(String objectName) throws Exception {
        //移除对象（文件）
        minioClient.removeObject(
                RemoveObjectArgs.builder()
                        .bucket(minioConfigurationProperties.getBucket())
                        .object(objectName)
                        .build());
    }

    public InputStream getObject(String objectName) throws IOException, InvalidKeyException, InvalidResponseException,
            InsufficientDataException, NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        //获取文件，下载文件
        minioClient.statObject(StatObjectArgs.builder()
                .bucket(minioConfigurationProperties.getBucket())
                .object(objectName)
                .build());
        return minioClient.getObject(
                GetObjectArgs.builder()
                        .bucket(minioConfigurationProperties.getBucket())
                        .object(objectName)
                        .build());
    }
}
