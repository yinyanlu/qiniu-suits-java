package com.qiniu.service.datasource;

import com.qiniu.common.Zone;
import com.qiniu.http.Response;
import com.qiniu.model.ListBucketParams;
import com.qiniu.model.ListResult;
import com.qiniu.sdk.BucketManager;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ListBucketTest {

    private ListBucket listBucket;
    private BucketManager bucketManager;
    private String bucket;
    private int unitLen;
    private int version;

    @Before
    public void init() throws Exception {
        ListBucketParams listBucketParams = new ListBucketParams("resources/.qiniu.properties");
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        Auth auth = Auth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());
        String resultFileDir = listBucketParams.getResultFileDir();
        String customPrefix = listBucketParams.getCustomPrefix();
        List<String> antiPrefix = listBucketParams.getAntiPrefix();
        this.bucket = listBucketParams.getBucket();
        this.version = listBucketParams.getVersion();
        this.unitLen = listBucketParams.getUnitLen();
        this.unitLen = (version == 1 && unitLen > 1000) ? unitLen%1000 : unitLen;
        this.bucketManager = new BucketManager(auth, configuration);
        this.listBucket = new ListBucket(auth, configuration, bucket, unitLen, version, customPrefix,
                antiPrefix, 1);
    }

    @Test
    public void testGetListResult() throws Exception {
        Response response = listBucket.list(bucketManager, "e", "", "", unitLen);
        ListResult listResult = listBucket.getListResult(response, version);
        response.close();
        Assert.assertTrue(listResult.isValid());
        Assert.assertEquals(1, listResult.fileInfoList.size());
    }

    @Test
    public void testStraightList() throws IOException {
        listBucket.straightlyList("v2", "", "==", null, false);
    }

}