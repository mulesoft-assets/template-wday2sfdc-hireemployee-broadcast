/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.kicks;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.mule.util.DateUtils;

import com.workday.hr.EffectiveAndUpdatedDateTimeDataType;
import com.workday.hr.GetWorkersRequestType;
import com.workday.hr.TransactionLogCriteriaType;
import com.workday.hr.TransactionLogTypeObjectIDType;
import com.workday.hr.TransactionLogTypeObjectType;
import com.workday.hr.TransactionTypeReferencesType;
import com.workday.hr.WorkerRequestCriteriaType;
import com.workday.hr.WorkerResponseGroupType;

public class HireEmployeeRequest {

	public static GetWorkersRequestType create(Date startDate, int periodInMillis) throws ParseException, DatatypeConfigurationException {

		/*
		 * Set data range for events
		 */

        EffectiveAndUpdatedDateTimeDataType dateRangeData = new EffectiveAndUpdatedDateTimeDataType();

		Calendar cal = Calendar.getInstance();
		cal.setTime(startDate);
		cal.add(Calendar.SECOND, - periodInMillis / 1000);

		Date current = new Date();
		if (!DateUtils.isSameDay(startDate, current )) {
			startDate = current;
		}

		dateRangeData.setUpdatedFrom(xmlDate(cal.getTime()));
        dateRangeData.setUpdatedThrough(xmlDate(startDate));

        dateRangeData.setEffectiveFrom(xmlDate(cal.getTime()));
        dateRangeData.setEffectiveThrough(xmlDate(startDate));
        
		/*
		 * Set event type criteria filter
		 */

        // Hire Employee
        TransactionTypeReferencesType transactionTypeReferences = new TransactionTypeReferencesType();
        TransactionLogTypeObjectType transactionLogTypeObjectType = new TransactionLogTypeObjectType();
        TransactionLogTypeObjectIDType idType = new TransactionLogTypeObjectIDType();

        idType.setType("Business_Process_Type");
        idType.setValue("Hire Employee");

        transactionLogTypeObjectType.getID().add(idType);
        transactionTypeReferences.getTransactionTypeReference().add(transactionLogTypeObjectType);
        TransactionLogCriteriaType transactionLogCriteria = new TransactionLogCriteriaType();
        transactionLogCriteria.setTransactionDateRangeData(dateRangeData);
        transactionLogCriteria.setTransactionTypeReferences(transactionTypeReferences);

		WorkerRequestCriteriaType workerRequestCriteria = new WorkerRequestCriteriaType();
		workerRequestCriteria.getTransactionLogCriteriaData().add(transactionLogCriteria);
        workerRequestCriteria.setExcludeInactiveWorkers(true);
        workerRequestCriteria.setExcludeEmployees(false);
        workerRequestCriteria.setExcludeContingentWorkers(true);

        GetWorkersRequestType getWorkersType = new GetWorkersRequestType();
        getWorkersType.setRequestCriteria(workerRequestCriteria);

		WorkerResponseGroupType resGroup = new WorkerResponseGroupType();
		resGroup.setIncludeRoles(true);	
		resGroup.setIncludePersonalInformation(true);
		resGroup.setIncludeOrganizations(true);
		resGroup.setIncludeEmploymentInformation(true);
		resGroup.setIncludeReference(true);
		getWorkersType.setResponseGroup(resGroup);

		return getWorkersType;
	}

	private static XMLGregorianCalendar xmlDate(Date date) throws DatatypeConfigurationException {
		GregorianCalendar gregorianCalendar = (GregorianCalendar) GregorianCalendar.getInstance();
		gregorianCalendar.setTime(date);
		return DatatypeFactory.newInstance().newXMLGregorianCalendar(gregorianCalendar);
	}

}
