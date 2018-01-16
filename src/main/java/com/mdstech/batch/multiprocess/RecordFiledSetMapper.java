package com.mdstech.batch.multiprocess;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

public class RecordFiledSetMapper implements FieldSetMapper<CustomerDomain> {

    @Override
    public CustomerDomain mapFieldSet(FieldSet fieldSet) throws BindException {
        CustomerDomain customerDomain = new CustomerDomain();
        customerDomain.setCustomerId(fieldSet.readInt("customerId"));
        customerDomain.setName(fieldSet.readString("name"));
        customerDomain.setHouseNo(fieldSet.readInt("houseNo"));
        customerDomain.setStreetName(fieldSet.readString("streetName"));
        customerDomain.setState(fieldSet.readString("state"));
        customerDomain.setZipCode(fieldSet.readInt("zipCode"));
        return customerDomain;
    }
}
