/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates.integration;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.xml.datatype.DatatypeConfigurationException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mule.MessageExchangePattern;
import org.mule.api.MuleEvent;
import org.mule.api.MuleException;
import org.mule.context.notification.NotificationException;
import org.mule.processor.chain.SubflowInterceptingChainLifecycleWrapper;
import org.mule.templates.utils.Employee;

import com.mulesoft.module.batch.BatchTestHelper;
import com.workday.hr.EmployeeGetType;
import com.workday.hr.EmployeeReferenceType;
import com.workday.hr.ExternalIntegrationIDReferenceDataType;
import com.workday.hr.IDType;
import com.workday.staffing.EventClassificationSubcategoryObjectIDType;
import com.workday.staffing.EventClassificationSubcategoryObjectType;
import com.workday.staffing.TerminateEmployeeDataType;
import com.workday.staffing.TerminateEmployeeRequestType;
import com.workday.staffing.TerminateEventDataType;

/**
 * The objective of this class is to validate the correct behavior of the flows
 * for this Anypoint Template that make calls to external systems.
 */
@SuppressWarnings("deprecation")
public class BusinessLogicIT extends AbstractTemplateTestCase {

	protected static final String PATH_TO_TEST_PROPERTIES = "./src/test/resources/mule.test.properties";
    private static final long TIMEOUT_MILLIS = 180000;
    private static final long DELAY_MILLIS = 500;
	private static final String TEMPLATE_PREFIX = "wday2sfdc-hireempl-broad";
    private static String WDAY_TERMINATION_ID;
	private static String WDAY_EXT_ID;
    private BatchTestHelper helper;
    private String EXT_ID, EMAIL = "wday2sfdc-hireempl@gmailtest.com";
	private Employee employee;
	
    @BeforeClass
    public static void beforeTestClass() {

        System.setProperty("poll.startDelayMillis", "8000");
        System.setProperty("poll.frequencyMillis", "30000");
        Date initialDate = new Date(System.currentTimeMillis());
        Calendar cal = Calendar.getInstance();
        cal.setTime(initialDate);
        System.setProperty(
        "watermark.default.expression", 
        "#[groovy: new GregorianCalendar("
        + cal.get(Calendar.YEAR) + ","
        + cal.get(Calendar.MONTH) + ","
        + cal.get(Calendar.DAY_OF_MONTH) + ","
        + cal.get(Calendar.HOUR_OF_DAY) + ","
        + cal.get(Calendar.MINUTE) + ","
        + cal.get(Calendar.SECOND) + ") ]");
        
    	final Properties props = new Properties();
    	try {
    	props.load(new FileInputStream(PATH_TO_TEST_PROPERTIES));
    	} catch (Exception e) {
    		System.out.println("Error occured while reading mule.test.properties" + e);
    	} 
    	
        WDAY_TERMINATION_ID = props.getProperty("wday.termination.id");
    	WDAY_EXT_ID = props.getProperty("wday.ext.id");
    }

    @Before
    public void setUp() throws Exception {
        stopFlowSchedulers(POLL_FLOW_NAME);

        helper = new BatchTestHelper(muleContext);
        registerListeners();
        
        createTestDataInSandBox();
    }
    
    @After
    public void tearDown() throws MuleException, Exception {
    	deleteTestDataFromSandBox();
    }
    
    private void createTestDataInSandBox() throws MuleException, Exception {
		SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("hireNewEmployee");
		flow.initialise();
		logger.info("creating a workday employee...");
		try {
			flow.process(getTestEvent(prepareNewHire(), MessageExchangePattern.REQUEST_RESPONSE));						
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
    
    private Object prepareNewHire(){
		EXT_ID = TEMPLATE_PREFIX + System.currentTimeMillis();
		logger.info("employee name: " + EXT_ID);
		employee = new Employee(EXT_ID, "Willis1", EMAIL, "650-2323", "999 Main St", "San Francisco", "USA-CA", "94105", "USA", "o7aHYfwG", 
				"2014-04-17T07:00:00.000+02:00", "2014-04-21T07:00:00.000+02:00", "QA Engineer", "San_Francisco_site", "Regular", "Full_time", "Salary", "USD", "140000", "Annual", "39905", "21440", EXT_ID);
		return employee;
	}

    private void registerListeners() throws NotificationException {
        muleContext.registerListener(pipelineListener);
    }

    @SuppressWarnings({ "unchecked" })
    @Test
    public void testMainFlow() throws Exception {
    	Thread.sleep(20000);
        runSchedulersOnce(POLL_FLOW_NAME);
        waitForPollToRun();
        helper.awaitJobTermination(TIMEOUT_MILLIS, DELAY_MILLIS);
        helper.assertJobWasSuccessful();
        
        SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("getSfdcUser");
		flow.initialise();				
		MuleEvent response = flow.process(getTestEvent(EMAIL, MessageExchangePattern.REQUEST_RESPONSE));
		Map<String, Object> payload = (Map<String, Object>)response.getMessage().getPayload();
				
		Assert.assertEquals("The email adress should be synced.",EMAIL,payload.get("Email"));
		Assert.assertEquals("The firstname should be synced.",EXT_ID, payload.get("FirstName"));
		Assert.assertEquals("The type should be set to User.","User",payload.get("type"));
    }
    
    private void deleteTestDataFromSandBox() throws MuleException, Exception {
    	logger.info("deleting test data...");
		
    	// Delete the created users in Workday
    	SubflowInterceptingChainLifecycleWrapper flow = getSubFlow("getWorkdaytoTerminateFlow");
		flow.initialise();
		
		try {
			MuleEvent response = flow.process(getTestEvent(getEmployee(), MessageExchangePattern.REQUEST_RESPONSE));			
			flow = getSubFlow("terminateWorkdayEmployee");
			flow.initialise();
			flow.process(getTestEvent(prepareTerminate(response), MessageExchangePattern.REQUEST_RESPONSE));								
		} catch (Exception e) {
			e.printStackTrace();
		}		

	}
    
    private EmployeeGetType getEmployee(){
		EmployeeGetType get = new EmployeeGetType();
		EmployeeReferenceType empRef = new EmployeeReferenceType();					
		ExternalIntegrationIDReferenceDataType value = new ExternalIntegrationIDReferenceDataType();
		IDType idType = new IDType();
		value.setID(idType);
		// use an existing external ID just for matching purpose
		idType.setSystemID(WDAY_EXT_ID);
		idType.setValue(EXT_ID);			
		empRef.setIntegrationIDReference(value);
		get.setEmployeeReference(empRef);		
		return get;
	}
	
	private TerminateEmployeeRequestType prepareTerminate(MuleEvent response) throws DatatypeConfigurationException{
		TerminateEmployeeRequestType req = (TerminateEmployeeRequestType) response.getMessage().getPayload();
		TerminateEmployeeDataType eeData = req.getTerminateEmployeeData();		
		TerminateEventDataType event = new TerminateEventDataType();
		eeData.setTerminationDate(new GregorianCalendar());
		EventClassificationSubcategoryObjectType prim = new EventClassificationSubcategoryObjectType();
		List<EventClassificationSubcategoryObjectIDType> list = new ArrayList<EventClassificationSubcategoryObjectIDType>();
		EventClassificationSubcategoryObjectIDType id = new EventClassificationSubcategoryObjectIDType();
		id.setType("WID");
		id.setValue(WDAY_TERMINATION_ID);
		list.add(id);
		prim.setID(list);
		event.setPrimaryReasonReference(prim);
		eeData.setTerminateEventData(event );
		return req;		
	}
}
