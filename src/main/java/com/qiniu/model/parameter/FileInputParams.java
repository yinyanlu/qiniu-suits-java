package com.qiniu.model.parameter;

import com.qiniu.common.QiniuException;
import com.qiniu.util.StringUtils;

public class FileInputParams extends CommonParams {

    private String parseType;
    private String separator;
    private String filePath;
    private String keyIndex = "";
    private String unitLen = "";

    public FileInputParams(String[] args) throws Exception {
        super(args);
        try { this.parseType = getParamFromArgs("parse-type"); } catch (Exception e) {}
        try { this.separator = getParamFromArgs("separator"); } catch (Exception e) {}
        this.filePath = getParamFromArgs("file-path");
        try { this.keyIndex = getParamFromArgs("key-index"); } catch (Exception e) {}
        try { this.unitLen = getParamFromArgs("unit-len"); } catch (Exception e) {}
    }

    public FileInputParams(String configFileName) throws Exception {
        super(configFileName);
        try { this.parseType = getParamFromConfig("parser-type"); } catch (Exception e) {}
        try { this.separator = getParamFromConfig("separator"); } catch (Exception e) { this.separator = ""; }
        this.filePath = getParamFromConfig("file-path");
        try { this.keyIndex = getParamFromConfig("key-index"); } catch (Exception e) {}
        try { this.unitLen = getParamFromConfig("unit-len"); } catch (Exception e) {}
    }

    public String getParseType() {
        if (StringUtils.isNullOrEmpty(parseType)) {
            System.out.println("no incorrect parse type, it will use \"json\" as default.");
            return "json";
        } else {
            return parseType;
        }
    }

    public String getSeparator() {
        if (StringUtils.isNullOrEmpty(separator)) {
            System.out.println("no incorrect separator, it will use \"\t\" as default.");
            return "\t";
        } else {
            return separator;
        }
    }

    public String getFilePath() throws QiniuException {
        if (StringUtils.isNullOrEmpty(filePath)) {
            throw new QiniuException(null, "no incorrect file-path, please set it.");
        } else {
            return filePath;
        }
    }

    public int getKeyIndex() {
        if (keyIndex.matches("[\\d]")) {
            return Integer.valueOf(keyIndex);
        } else {
            System.out.println("no incorrect key index, it will use 0 as default.");
            return 0;
        }
    }

    public int getUnitLen() {
        if (unitLen.matches("\\d+")) {
            return Integer.valueOf(unitLen);
        } else {
            System.out.println("no incorrect unit-len, it will use 1000 as default.");
            return 1000;
        }
    }
}
