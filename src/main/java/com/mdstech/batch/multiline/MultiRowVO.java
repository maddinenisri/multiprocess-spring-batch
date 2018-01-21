package com.mdstech.batch.multiline;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class MultiRowVO {
    public enum OptionType {ZERO, ONE_FOURTH, HALF, THREE_FOURTH, FULL};
    private String key;
    private String groupKey;
    private Map<String, ColumnVO> fieldMap = new HashMap<>();

    public void setValues(OptionType optionType, String...values) {
        int index = 6;
        for(String value: values) {
            String key = String.format("field%d", index);
            if(!fieldMap.containsKey(key)) {
                fieldMap.put(key, new ColumnVO());
            }
            setValue(optionType, fieldMap.get(key),value);
            index++;
        }
    }

    private void setValue(OptionType optionType, ColumnVO columnVO, String value) {
        switch (optionType) {
            case ZERO:
                columnVO.setZero(value);
                break;
            case ONE_FOURTH:
                columnVO.setOnefourth(value);
                break;
            case HALF:
                columnVO.setHalf(value);
                break;
            case THREE_FOURTH:
                columnVO.setThreefourth(value);
                break;
            case FULL:
                columnVO.setFull(value);
                break;

        }
    }

    public List<RecordVO> getRecords() {
        List<RecordVO> recordVOS =  new ArrayList<>();
        for(ColumnVO columnVO : fieldMap.values()) {
            RecordVO recordVO = new RecordVO();
            recordVO.setKey(getKey());
            recordVO.setGroupKey(getGroupKey());
            recordVO.setFull(columnVO.getFull());
            recordVO.setHalf(columnVO.getHalf());
            recordVO.setOnefourth(columnVO.getOnefourth());
            recordVO.setThreefourth(columnVO.getThreefourth());
            recordVO.setZero(columnVO.getZero());
            recordVOS.add(recordVO);
        }
        return recordVOS;
    }
}
