package com.mdstech.batch;

import com.mdstech.batch.common.config.ApplicationConfiguration;
import com.mdstech.batch.common.config.StandaloneInfrastructureConfiguration;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {ApplicationConfiguration.class, StandaloneInfrastructureConfiguration.class})
public class SpringUnitTestCaseHelper {
}