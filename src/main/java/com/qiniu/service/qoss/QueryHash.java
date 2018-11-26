package com.qiniu.service.qoss;

import com.qiniu.common.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.model.qoss.Qhash;
import com.qiniu.service.interfaces.IQossProcess;
import com.qiniu.sdk.FileInfo;
import com.qiniu.util.HttpResponseUtils;
import com.qiniu.util.JsonConvertUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QueryHash implements IQossProcess, Cloneable {

    private String domain;
    private FileChecker fileChecker;
    private String processName;
    private int retryCount = 3;
    protected String resultFileDir;
    private FileMap fileMap;

    private void initBaseParams(String domain) {
        this.processName = "hash";
        this.domain = domain;
    }

    public QueryHash(String domain, String resultFileDir) {
        initBaseParams(domain);
        this.resultFileDir = resultFileDir;
        this.fileChecker = new FileChecker(null);
        this.fileMap = new FileMap();
    }

    public QueryHash(String domain, String resultFileDir, int resultFileIndex) throws IOException {
        this(domain, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public QueryHash getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QueryHash queryHash = (QueryHash)super.clone();
        queryHash.fileChecker = new FileChecker(null);
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

    public String getInfo() {
        return domain;
    }

    public Qhash singleWithRetry(FileInfo fileInfo, int retryCount) throws QiniuException {

        Qhash qhash = null;
        try {
            qhash = fileChecker.getQHash(domain, fileInfo.key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    qhash = fileChecker.getQHash(domain, fileInfo.key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return qhash;
    }

    public void processFile(List<FileInfo> fileInfoList) throws QiniuException {

        fileInfoList = fileInfoList == null ? null : fileInfoList.parallelStream()
                .filter(Objects::nonNull).collect(Collectors.toList());
        if (fileInfoList == null || fileInfoList.size() == 0) return;
        List<String> resultList = new ArrayList<>();
        for (FileInfo fileInfo : fileInfoList) {
            try {
                Qhash qhash = singleWithRetry(fileInfo, retryCount);
                if (qhash != null) resultList.add(fileInfo.key + "\t" + JsonConvertUtils.toJsonWithoutUrlEscape(qhash));
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, processName, getInfo() + "\t" + fileInfo.key);
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
