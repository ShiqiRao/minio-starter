package com.example.minio;

import io.minio.*;
import io.minio.errors.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class TestCases {
    private MinioClient minioClient;

    @BeforeEach
    void setup() {
        minioClient = MinioClient.builder()
                .endpoint("http://127.0.0.1:9000")
                .credentials("minioadmin", "minioadmin")
                .build();
    }

    @Test
    void testCreateBuckets() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException,
            NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        // Create bucket 'my-bucketname' if it doesn`t exist.
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket("my-bucketname").build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket("my-bucketname").build());
            System.out.println("my-bucketname is created successfully");
        }

        // Create bucket 'my-bucketname-in-eu' in 'eu-west-1' region if it doesn't exist.
        if (!minioClient.bucketExists(
                BucketExistsArgs.builder().bucket("my-bucketname-in-eu").build())) {
            minioClient.makeBucket(
                    MakeBucketArgs.builder().bucket("my-bucketname-in-eu").region("eu-west-1").build());
            System.out.println("my-bucketname-in-eu is created successfully");
        }
    }

    @Test
    void testDeleteBuckets() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException,
            NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        boolean found = minioClient.bucketExists(BucketExistsArgs.builder().bucket("my-bucketname").build());
        if (found) {
            minioClient.removeBucket(RemoveBucketArgs.builder().bucket("my-bucketname-in-eu").build());
            System.out.println("my-bucketname is removed successfully");
        } else {
            System.out.println("my-bucketname does not exist");
        }
    }

    @Test
    void testUploadObject() throws IOException, ServerException, InsufficientDataException, InternalException,
            InvalidResponseException, InvalidKeyException, NoSuchAlgorithmException, XmlParserException, ErrorResponseException {
        minioClient.uploadObject(
                UploadObjectArgs.builder()
                        .bucket("my-bucketname")
                        .object("my-filename")
                        .filename("my-filename")
                        .build());
    }

    @Test
    void testRemoveObject() throws IOException, InvalidKeyException, InvalidResponseException, InsufficientDataException,
            NoSuchAlgorithmException, ServerException, InternalException, XmlParserException, ErrorResponseException {
        minioClient.removeObject(
                RemoveObjectArgs.builder()
                        .bucket("my-bucketname")
                        .object("my-filename")
                        .build());
    }
}
