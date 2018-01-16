package com.mdstech.batch.multiprocess;

import lombok.Data;

import javax.persistence.*;
import java.io.Serializable;
import java.util.Date;

@Data
@Entity
@Table(name = "CUSTOMER_LARGE")
public class CustomerDomain implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private Long id;

    @Column(name="ID_CUSTOMER")
    private Integer customerId;

    @Column(name="NAME")
    private String name;

    @Column(name="HOUSE_NO")
    private Integer houseNo;

    @Column(name="ST_NAME")
    private String streetName;

    @Column(name="STATE")
    private String state;

    @Column(name="ZIP_CODE")
    private Integer zipCode;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="DT_CREATED")
    private Date createdDate;

    @Temporal(TemporalType.TIMESTAMP)
    @Column(name="DT_LST_UPDT")
    private Date lastUpdatedDate;
}