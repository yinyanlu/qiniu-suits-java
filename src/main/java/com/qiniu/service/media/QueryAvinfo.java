package com.qiniu.service.media;

import com.qiniu.persistence.FileMap;
import com.qiniu.common.QiniuException;
import com.qiniu.service.interfaces.ILineProcess;
import com.qiniu.util.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryAvinfo implements ILineProcess<Map<String, String>>, Cloneable {

    private String domain;
    private boolean https;
    private Auth srcAuth;
    private MediaManager mediaManager;
    private String processName;
    private int retryCount;
    protected String resultFileDir;
    private FileMap fileMap;

    private void initBaseParams(String domain) {
        this.processName = "avinfo";
        this.domain = domain;
    }

    public QueryAvinfo(String domain, String resultFileDir) {
        initBaseParams(domain);
        this.resultFileDir = resultFileDir;
        this.mediaManager = new MediaManager(https, srcAuth);
        this.fileMap = new FileMap();
    }

    public QueryAvinfo(String domain, String resultFileDir, int resultFileIndex) throws IOException {
        this(domain, resultFileDir);
        this.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
    }

    public void setOptions(boolean https, Auth srcAuth) {
        this.https = https;
        this.srcAuth = srcAuth;
    }

    public QueryAvinfo getNewInstance(int resultFileIndex) throws CloneNotSupportedException {
        QueryAvinfo queryAvinfo = (QueryAvinfo)super.clone();
        queryAvinfo.mediaManager = new MediaManager(https, srcAuth);
        queryAvinfo.fileMap = new FileMap();
        try {
            queryAvinfo.fileMap.initWriter(resultFileDir, processName, resultFileIndex);
        } catch (IOException e) {
            throw new CloneNotSupportedException("init writer failed.");
        }
        return queryAvinfo;
    }

    public void setBatch(boolean batch) {}

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public String getProcessName() {
        return this.processName;
    }

    public String singleWithRetry(String key, int retryCount) throws QiniuException {

        String avinfo = null;
        try {
            avinfo = mediaManager.getAvinfoBody(domain, key);
        } catch (QiniuException e1) {
            HttpResponseUtils.checkRetryCount(e1, retryCount);
            while (retryCount > 0) {
                try {
                    avinfo = mediaManager.getAvinfoBody(domain, key);
                    retryCount = 0;
                } catch (QiniuException e2) {
                    retryCount = HttpResponseUtils.getNextRetryCount(e2, retryCount);
                }
            }
        }

        return avinfo;
    }

    public void processLine(List<Map<String, String>> lineList) throws QiniuException {

        List<String> resultList = new ArrayList<>();
        for (Map<String, String> line : lineList) {
            try {
                String avinfo = singleWithRetry(line.get("key"), retryCount);
                if (avinfo != null) resultList.add(line.get("key") + "\t" + avinfo);
                else throw new QiniuException(null, "empty avinfo");
            } catch (QiniuException e) {
                HttpResponseUtils.processException(e, fileMap, line.get("key"));
            }
        }
        if (resultList.size() > 0) fileMap.writeSuccess(String.join("\n", resultList));
    }

    public void closeResource() {
        fileMap.closeWriter();
    }
}
