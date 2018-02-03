package com.mdstech.batch.multiline;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class SimpleItemMapper implements FieldSetMapper<SimpleVO> {

    @Override
    public SimpleVO mapFieldSet(FieldSet fieldSet) throws BindException {
        SimpleVO simpleVO = new SimpleVO();
        simpleVO.setKey(String.format("%s_%s", fieldSet.readString(0), fieldSet.readString(2)));
        return simpleVO;
    }
}
