package com.qiniu.examples;

import com.qiniu.common.*;
import com.qiniu.interfaces.IBucketProcess;
import com.qiniu.interfaces.IOssFileProcess;
import com.qiniu.model.*;
import com.qiniu.service.FileLine.NothingProcess;
import com.qiniu.service.impl.*;
import com.qiniu.storage.Configuration;

public class ListBucketMain {

    public static void main(String[] args) throws Exception {

        String configFile = ".qiniu.properties";
        boolean paramFromConfig = (args == null || args.length == 0);
        ListBucketParams listBucketParams = paramFromConfig ?
                new ListBucketParams(configFile) : new ListBucketParams(args);
        String accessKey = listBucketParams.getAccessKey();
        String secretKey = listBucketParams.getSecretKey();
        String bucket = listBucketParams.getBucket();
        String resultFileDir = listBucketParams.getResultFileDir();
        int threads = listBucketParams.getThreads();
        int version = listBucketParams.getVersion();
        boolean withParallel = listBucketParams.getWithParallel();
        int secondLevel = listBucketParams.getLevel();
        String process = listBucketParams.getProcess();

        FileReaderAndWriterMap listFileReaderAndWriterMap = new FileReaderAndWriterMap();
        listFileReaderAndWriterMap.initWriter(resultFileDir, listBucketParams.getSelfName());
        FileReaderAndWriterMap processFileReaderAndWriterMap = new FileReaderAndWriterMap();
        IOssFileProcess iOssFileProcessor = new NothingProcess();

        QiniuAuth auth = QiniuAuth.create(accessKey, secretKey);
        Configuration configuration = new Configuration(Zone.autoZone());

        if (!"".equals(process) && !"no".equals(process)) {

            switch (process) {
                // isBiggerThan 标志为 true 时，在 pointTime 时间点之前的记录进行处理，isBiggerThan 标志为 false 时，在 pointTime 时间点之后的记录进行处理。
                case "status":
                    FileStatusParams fileStatusParams = paramFromConfig ? new FileStatusParams(configFile) : new FileStatusParams(args);
                    PointTimeParams pointTimeParams = fileStatusParams.getPointTimeParams();
                    processFileReaderAndWriterMap.initWriter(resultFileDir, process);
                    iOssFileProcessor = new ChangeFileStatusProcess(auth, fileStatusParams.getBucket(), fileStatusParams.getTargetStatus(),
                            processFileReaderAndWriterMap, pointTimeParams.getPointDate() + " " + pointTimeParams.getPointTime(),
                            pointTimeParams.getDirection());
                    break;
                case "type":
                    FileTypeParams fileTypeParams = paramFromConfig ? new FileTypeParams(configFile) : new FileTypeParams(args);
                    pointTimeParams = fileTypeParams.getPointTimeParams();
                    processFileReaderAndWriterMap.initWriter(resultFileDir, process);
                    iOssFileProcessor = new ChangeFileTypeProcess(auth, configuration, fileTypeParams.getBucket(), fileTypeParams.getTargetType(),
                            processFileReaderAndWriterMap, pointTimeParams.getPointDate() + " " + pointTimeParams.getPointTime(),
                            pointTimeParams.getDirection());
                    break;
                case "copy":
                    FileCopyParams fileCopyParams = paramFromConfig ? new FileCopyParams(configFile) : new FileCopyParams(args);
                    accessKey = "".equals(fileCopyParams.getAKey()) ? accessKey : fileCopyParams.getAKey();
                    secretKey = "".equals(fileCopyParams.getSKey()) ? secretKey : fileCopyParams.getSKey();
                    processFileReaderAndWriterMap.initWriter(resultFileDir, process);
                    iOssFileProcessor = new BucketCopyItemProcess(QiniuAuth.create(accessKey, secretKey), configuration, fileCopyParams.getSourceBucket(),
                            fileCopyParams.getTargetBucket(), fileCopyParams.getTargetKeyPrefix(), processFileReaderAndWriterMap);
                    break;
            }
        }

        IBucketProcess listBucketMultiProcessor = new ListBucketMultiProcess(auth, configuration, bucket, iOssFileProcessor,
                listFileReaderAndWriterMap, threads);

        if (version == 2)
            listBucketMultiProcessor.processBucketV2(withParallel, secondLevel == 2);
        else
            listBucketMultiProcessor.processBucket(secondLevel == 2);

        listFileReaderAndWriterMap.closeWriter();
        iOssFileProcessor.closeResource();
        listBucketMultiProcessor.closeResource();
    }
}