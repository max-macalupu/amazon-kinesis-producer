/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.producer.sample;

import com.amazonaws.services.kinesis.producer.sample.record.StreamAlert;
import com.amazonaws.services.kinesis.producer.sample.record.StreamAlertBody;
import com.amazonaws.services.kinesis.producer.sample.record.StreamAlertHeader;
import com.google.gson.Gson;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Random;
import java.util.UUID;

public class Utils {
    private static final Random RANDOM = new Random();
    
    /**
     * @return A random unsigned 128-bit int converted to a decimal string.
     */
    public static String randomExplicitHashKey() {
        return new BigInteger(128, RANDOM).toString(10);
    }
    
    /**
     * Generates a blob containing a UTF-8 string. The string begins with the
     * sequence number in decimal notation, followed by a space, followed by
     * padding.
     * 
     * @param sequenceNumber
     *            The sequence number to place at the beginning of the record
     *            data.
     * @param totalLen
     *            Total length of the data. After the sequence number, padding
     *            is added until this length is reached.
     * @return ByteBuffer containing the blob
     */
    public static ByteBuffer generateData(long sequenceNumber, int totalLen) {
        StringBuilder sb = new StringBuilder();
        sb.append(new Gson().toJson(getStreamAlert()));
        System.out.println(sb.toString());
//        sb.append(Long.toString(sequenceNumber));
//        sb.append(" ");
//        while (sb.length() < totalLen) {
//            sb.append("mx");
//        }
        try {
            return ByteBuffer.wrap(sb.toString().getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private static StreamAlert getStreamAlert() {
        StreamAlertBody alertBody = new StreamAlertBody();
        alertBody.setAlertId(UUID.randomUUID().toString());
        alertBody.setAlertTime(new Date().toString());
        alertBody.setCategory("CATEGORY_01");
        alertBody.setSubCategory("SUB_CATEGORY_01");
        alertBody.setSubscriberId(UUID.randomUUID().toString());

        StreamAlertHeader alertHeader = new StreamAlertHeader();
        alertHeader.setEnvironment("DEV");
        alertHeader.setHostId("127.0.0.1");
        alertHeader.setRequestType("REQUEST_TYPE");
        alertHeader.setRequestId(UUID.randomUUID().toString());
        alertHeader.setSourceApp("test");
        alertHeader.setTime(new Date().toString());

        StreamAlert streamAlert = new StreamAlert();
        streamAlert.setBody(alertBody);
        streamAlert.setHeader(alertHeader);

        return streamAlert;
    }
}
