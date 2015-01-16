/**
 * Mule Anypoint Template
 *
 * Copyright (c) MuleSoft, Inc.  All rights reserved.  http://www.mulesoft.com
 */

package org.mule.templates;

import org.apache.commons.lang.RandomStringUtils;
import org.mule.api.MuleMessage;
import org.mule.api.routing.filter.Filter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The filter that's removing records from the payload with the same email address.
 *
 * @author aurel.medvegy
 */
public class WorkersDeduplicationFilter
        implements Filter {

    @SuppressWarnings("unchecked")
    @Override
    public boolean accept(MuleMessage message) {

        List<Map<String, String>> payload = (List<Map<String, String>>) message.getPayload();
        List<String> emails = new ArrayList<String>();

        Iterator<Map<String, String>> iterator = payload.iterator();

        while (iterator.hasNext()) {
            Map<String, String> next = iterator.next();
            String email = next.get("Email");

            if (emails.contains(email)) {
                iterator.remove();
            } else {
                emails.add(email);
                next.put("Username", RandomStringUtils.randomAlphabetic(8) + "@sf.com");
            }
        }

        return true;
    }
}
