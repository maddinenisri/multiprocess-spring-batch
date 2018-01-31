package com.mdstech.batch.multiline;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.transform.FieldSet;

public class MultiLineItemReader implements ItemReader<ContainerVO>, ItemStream {
    private FlatFileItemReader<FieldSet> delegate;
    private SequenceGenerator sequenceGenerator;

    public void setSequenceGenerator(SequenceGenerator sequenceGenerator) {
        this.sequenceGenerator = sequenceGenerator;
    }

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
                    setValue(containerVO.getMultiRowVO(), percent, line.readString(5), line.readString(6), line.readString(7), line.readString(8), line.readString(9), line.readString(10), line.readString(11), line.readString(12), line.readString(13), line.readString(14));
                }
                else if("0".equals(percent)) {
                    setValue(containerVO.getMultiRowVO(), percent, line.readString(5), line.readString(6), line.readString(7), line.readString(8), line.readString(9), line.readString(10), line.readString(11), line.readString(12), line.readString(13), line.readString(14));
                    return containerVO;
                }
                else {
                    setValue(containerVO.getMultiRowVO(), percent, line.readString(5), line.readString(6), line.readString(7), line.readString(8), line.readString(9), line.readString(10), line.readString(11), line.readString(12), line.readString(13), line.readString(14));
                }
            }
            else {
                containerVO = new ContainerVO();
                containerVO.setSimpleVO(new SimpleVO());
                Integer seq = sequenceGenerator.getNextSequence(line.readString(0), line.readString(2));
                containerVO.getSimpleVO().setKey(String.format("%s_%s", line.readString(0), line.readString(2)));
                containerVO.getSimpleVO().setSequence(seq);
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

    private void setValue(MultiRowVO multiRowVO, String percent, String... values) {
        switch(percent) {
            case "100":
                multiRowVO.setValues(MultiRowVO.OptionType.FULL, values);
                break;
            case "50":
                multiRowVO.setValues(MultiRowVO.OptionType.HALF, values);
                break;
            case "0":
                multiRowVO.setValues(MultiRowVO.OptionType.ZERO, values);
                break;
            case "75":
                multiRowVO.setValues(MultiRowVO.OptionType.THREE_FOURTH, values);
                break;
            case "25":
                multiRowVO.setValues(MultiRowVO.OptionType.ONE_FOURTH, values);
                break;
            default:
                break;
        }
    }
}
