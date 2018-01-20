package com.mdstech.batch.multiprocess;

import com.mdstech.batch.domain.CustomerDomain;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import java.util.List;

@Slf4j
public class CustomerWriter extends JpaItemWriter<CustomerDomain> {

    @Autowired
    public CustomerWriter(EntityManagerFactory entityManagerFactory) {
        super.setEntityManagerFactory(entityManagerFactory);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerWriter.class);
    @Override
    @Transactional
    protected void doWrite(EntityManager entityManager, List<? extends CustomerDomain> items) {
        for(CustomerDomain customerDomain : items) {
            entityManager
                    .persist(customerDomain);
        }
        entityManager.flush();
        entityManager.clear();
        LOGGER.debug("Persisted to DB by " + Thread.currentThread().getName());
    }
}
