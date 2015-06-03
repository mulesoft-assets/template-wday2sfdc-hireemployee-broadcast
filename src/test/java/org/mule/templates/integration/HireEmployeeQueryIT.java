/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import com.workday.hr.GetWorkersResponseType;
import com.workday.hr.WorkerType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleException;
import org.mule.api.MuleMessage;
import org.mule.api.lifecycle.InitialisationException;
import org.mule.processor.chain.InterceptingChainLifecycleWrapper;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import static org.junit.Assert.assertFalse;

/**
 * The objective of this class is to validate the correct behavior of the flows
 * for this Anypoint Template that make calls to external systems.
 */
public class HireEmployeeQueryIT extends AbstractTemplateTestCase {

    private static final Logger log = LogManager.getLogger(HireEmployeeQueryIT.class);

    private InterceptingChainLifecycleWrapper queryEmployeeFromWorkdayFlow;

    @BeforeClass
    public static void beforeTestClass() {
        System.setProperty("poll.startDelayMillis", "100");
        System.setProperty("poll.frequencyMillis", "10000");

        // Set default water-mark expression to current time
        System.clearProperty("watermark.default.expression");
        System.setProperty("watermark.default.expression", "#[groovy: new GregorianCalendar(2015, Calendar.MAY, 22, 13, 00, 00)]");
    }

    @Before
    public void setUp() throws MuleException {
        stopAutomaticPollTriggering();
        getAndInitializeFlows();
    }

    @After
    public void tearDown() {
    }

    private void stopAutomaticPollTriggering() throws MuleException {
        stopFlowSchedulers("triggerFlow");
    }

    private void getAndInitializeFlows() throws InitialisationException {
        // Flow for querying employees in Workday
        queryEmployeeFromWorkdayFlow = getSubFlow("queryWorkdayEmployeeFlow");
        queryEmployeeFromWorkdayFlow.initialise();
    }

    @Test
    public void testMainFlow() throws Exception {

    	GregorianCalendar date = new GregorianCalendar();
    	date.add(Calendar.SECOND, -(60 * 60 * 168));
        GetWorkersResponseType response = (GetWorkersResponseType) queryWorkdayEmployee(queryEmployeeFromWorkdayFlow, date);

        if (response.getResponseData() != null) {
            List<WorkerType> workers = response.getResponseData().getWorker();
            assertFalse("The response data should not be empty", workers.isEmpty());
            log.info("workers.size() = " + workers.size());
        }
    }

    private Object queryWorkdayEmployee(InterceptingChainLifecycleWrapper flow, GregorianCalendar date)
            throws MuleException, Exception {

        MuleMessage message = flow.process(getTestEvent(date, MessageExchangePattern.REQUEST_RESPONSE)).getMessage();
        return message.getPayload();
    }
}
