package com.qiniu.entry;

import com.qiniu.model.parameter.FileInputParams;
import com.qiniu.model.parameter.InputFieldParams;
import com.qiniu.model.parameter.ListFieldParams;
import com.qiniu.service.datasource.FileInput;
import com.qiniu.service.interfaces.ILineProcess;

import java.util.Map;

public class FileInputEntry {

    public static void run(boolean paramFromConfig, String[] args, String configFilePath) throws Exception {

        FileInputParams fileInputParams = paramFromConfig ? new FileInputParams(configFilePath) : new FileInputParams(args);
        String filePath = fileInputParams.getFilePath();
        String parseType = fileInputParams.getParseType();
        String separator = fileInputParams.getSeparator();
        boolean saveTotal = fileInputParams.getSaveTotal();
        String resultFormat = fileInputParams.getResultFormat();
        String resultSeparator = fileInputParams.getResultFormat();
        String resultFileDir = fileInputParams.getResultFileDir();
        int maxThreads = fileInputParams.getMaxThreads();
        int unitLen = fileInputParams.getUnitLen();
        String sourceFilePath = System.getProperty("user.dir") + System.getProperty("file.separator") + filePath;
        ILineProcess<Map<String, String>> lineProcessor = new ProcessorChoice(paramFromConfig, args, configFilePath)
                .getFileProcessor();
        Map<String, String> infoIndexMap = new InputInfoParser().getInfoIndexMap(fileInputParams);
        FileInput fileInput = new FileInput(parseType, separator, infoIndexMap, unitLen, resultFileDir);
        fileInput.setSaveTotalOptions(saveTotal, resultFormat, resultSeparator);
        InputFieldParams fieldParams = paramFromConfig ? new InputFieldParams(configFilePath) : new InputFieldParams(args);
        fileInput.process(maxThreads, sourceFilePath, fieldParams.getUsedFields(), lineProcessor);
        if (lineProcessor != null) lineProcessor.closeResource();
    }
}
