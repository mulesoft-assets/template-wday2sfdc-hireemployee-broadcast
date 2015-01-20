/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import com.mulesoft.module.batch.BatchTestHelper;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.context.notification.NotificationException;

/**
 * The objective of this class is to validate the correct behavior of the flows
 * for this Anypoint Template that make calls to external systems.
 */
public class BusinessLogicIT extends AbstractTemplateTestCase {

    private static final long TIMEOUT_MILLIS = 60000;
    private static final long DELAY_MILLIS = 500;
    private BatchTestHelper helper;

    @BeforeClass
    public static void beforeTestClass() {
        System.setProperty("poll.startDelayMillis", "8000");
        System.setProperty("poll.frequencyMillis", "30000");
        System.setProperty("watermark.default.expression", "#[groovy: new Date(System.currentTimeMillis() - 1000 * 60 * 60 * 168)]");
    }

    @Before
    public void setUp() throws Exception {
        stopFlowSchedulers(POLL_FLOW_NAME);

        helper = new BatchTestHelper(muleContext);
        registerListeners();
    }

    private void registerListeners() throws NotificationException {
        muleContext.registerListener(pipelineListener);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testMainFlow() throws Exception {
        runSchedulersOnce(POLL_FLOW_NAME);
        waitForPollToRun();
        helper.awaitJobTermination(TIMEOUT_MILLIS, DELAY_MILLIS);
        helper.assertJobWasSuccessful();
    }
}
