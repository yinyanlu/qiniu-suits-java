package com.qiniu.service.qoss;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.Auth;
import com.qiniu.util.HttpResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryHash implements ILineProcess<Map<String, String>>, Cloneable {

    private String domain;
    private boolean https;
    private Auth srcAuth;
    private String algorithm;
    private FileChecker fileChecker;
    private String processName;
    private int retryCount;
    protected String resultFileDir;
    private FileMap fileMap;

    private void initBaseParams(String domain) {
        this.processName = "hash";
        this.domain = domain;
    }

    public QueryHash(String domain, String resultFileDir) {
        initBaseParams(domain);
        this.resultFileDir = resultFileDir;
        this.fileChecker = new FileChecker(null, https, srcAuth);
        this.fileMap = new FileMap();
    }

    public QueryHash(String domain, String resultFileDir, int resultFileIndex) throws IOException {
        this(domain, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public void setOptions(String algorithm, boolean https, Auth srcAuth) {
        this.algorithm = algorithm;
        this.https = https;
        this.srcAuth = srcAuth;
        this.fileChecker = new FileChecker(algorithm, https, srcAuth);
    }

    public QueryHash getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(algorithm, https, srcAuth);
        queryHash.fileMap = new FileMap();
        try {
            queryHash.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryHash;
    }

    public void setBatch(boolean batch) {}

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String singleWithRetry(String key, int retryCount) throws QiniuException {

        String qhash = null;
        try {
            qhash = fileChecker.getQHashBody(domain, key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    qhash = fileChecker.getQHashBody(domain, key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return qhash;
    }

    public void processLine(List<Map<String, String>> fileInfoList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        for (Map<String, String> fileInfo : fileInfoList) {
            try {
                String qhash = singleWithRetry(fileInfo.get("key"), retryCount);
                if (qhash != null) resultList.add(fileInfo.get("key") + "\t" + qhash);
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, fileInfo.get("key"));
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
