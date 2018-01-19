package com.mdstech.batch.multiline;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.FieldSet;

public class MultiLineItemReader implements ItemReader<ContainerVO>, ItemStream {
    private FlatFileItemReader<FieldSet> delegate;

    public void setDelegate(FlatFileItemReader<FieldSet> delegate) {
        this.delegate = delegate;
    }

    @Override
    public ContainerVO read() throws Exception {

        ContainerVO containerVO = null;

        for (FieldSet line; (line = this.delegate.read()) != null;) {
            String prefix = line.readString(0);
            if (prefix.equals("1")) {
                String percent = line.readString(4);
                if(containerVO == null) {
                    MultiRowVO multiRowVO = new MultiRowVO();
                    containerVO = new ContainerVO();
                    containerVO.setMultiRowVO(multiRowVO);
                    containerVO.getMultiRowVO().setKey(prefix);
                    containerVO.getMultiRowVO().setGroupKey(line.readString(3));
                    setValue(containerVO.getMultiRowVO(), percent, line.readString(5));
                }
                else if("0".equals(percent)) {
                    setValue(containerVO.getMultiRowVO(), percent, line.readString(5));
                    return containerVO;
                }
                else {
                    setValue(containerVO.getMultiRowVO(), percent, line.readString(5));
                }
            }
            else {
                containerVO = new ContainerVO();
                containerVO.setSimpleVO(new SimpleVO());
                containerVO.getSimpleVO().setKey(line.readString(2));
                return containerVO;
            }
        }
        return null;
    }

    @Override
    public void close() throws ItemStreamException {
        this.delegate.close();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.delegate.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        this.delegate.update(executionContext);
    }

    private void setValue(MultiRowVO multiRowVO, String percent, String value) {
        switch(percent) {
            case "100":
                multiRowVO.setFull(value);
                break;
            case "50":
                multiRowVO.setHalf(value);
                break;
            case "0":
                multiRowVO.setZero(value);
                break;
            case "75":
                multiRowVO.setThreefourth(value);
                break;
            case "25":
                multiRowVO.setOnefourth(value);
                break;
            default:
                break;
        }
    }
}
