/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.event.publisher.sample;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.json.JSONObject;
import org.wso2.carbon.base.MultitenantConstants;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils;
import org.wso2.carbon.identity.base.IdentityRuntimeException;
import org.wso2.carbon.identity.data.publisher.application.authentication.AbstractAuthenticationDataPublisher;
import org.wso2.carbon.identity.data.publisher.application.authentication.AuthPublisherConstants;
import org.wso2.carbon.identity.data.publisher.application.authentication.AuthnDataPublisherUtils;
import org.wso2.carbon.identity.data.publisher.application.authentication.internal
        .AuthenticationDataPublisherDataHolder;
import org.wso2.carbon.identity.data.publisher.application.authentication.model.AuthenticationData;
import org.wso2.carbon.identity.data.publisher.application.authentication.model.SessionData;
import org.wso2.event.publisher.sample.internal.SampleEventPublisherDataHolder;

import java.io.IOException;

public class SampleEventPublisher extends AbstractAuthenticationDataPublisher {

    private static final String CUSTOM_STREAM_NAME = "org.wso2.is.analytics.stream.CustomAuthData:1.0.0";
    private static Log log = LogFactory.getLog(SampleEventPublisher.class);

    @Override
    public void doPublishAuthenticationStepSuccess(AuthenticationData authenticationData) {

        log.info("doPublishAuthenticationStepSuccess");

    }

    @Override
    public void doPublishAuthenticationStepFailure(AuthenticationData authenticationData) {

        log.info("doPublishAuthenticationStepFailure");

    }

    @Override
    public void doPublishAuthenticationSuccess(AuthenticationData authenticationData) {

        log.info("doPublishAuthenticationSuccess");

        // Publishing the authentication data as a wso2 event and an HTTP request.
        publishEventUsingAdapter(authenticationData);
        publishEventUsingHttpClient(authenticationData);

    }

    @Override
    public void doPublishAuthenticationFailure(AuthenticationData authenticationData) {

        log.info("doPublishAuthenticationFailure");

    }

    @Override
    public void doPublishSessionCreation(SessionData sessionData) {

        log.info("doPublishSessionCreation");

    }

    @Override
    public void doPublishSessionUpdate(SessionData sessionData) {

        log.info("doPublishSessionUpdate");

    }

    @Override
    public void doPublishSessionTermination(SessionData sessionData) {

        log.info("doPublishSessionTermination");

    }

    /**
     * This method will publish the custom event using the wso2 event adapter. This requires,
     * a stream to be defined in <IS-HOME>/repository/deployment/server/eventstreams
     * and a publisher to be defined in <IS-HOME>/repository/deployment/server/eventpublishers
     *
     * @param authenticationData
     */
    private void publishEventUsingAdapter(AuthenticationData authenticationData) {
        try {

            // Populating the event attributes
            Object[] payloadData = new Object[5];
            payloadData[0] = "my_amplitude_api_key";
            payloadData[1] = authenticationData.getEventType();
            payloadData[2] = AuthnDataPublisherUtils.replaceIfNotAvailable(AuthPublisherConstants.CONFIG_PREFIX +
                    AuthPublisherConstants.USERNAME, authenticationData.getUsername());
            String authenticationResult = authenticationData.isAuthnSuccess() ? "SUCCESS" : "FAILURE";
            payloadData[3] = authenticationResult;
            payloadData[4] = AuthnDataPublisherUtils.replaceIfNotAvailable(AuthPublisherConstants.CONFIG_PREFIX +
                    AuthPublisherConstants.SERVICE_PROVIDER, authenticationData.getServiceProvider());

            // Publishing the event using the wso2 event adapters.
            String[] publishingDomains = (String[]) authenticationData.getParameter(AuthPublisherConstants.TENANT_ID);
            if (publishingDomains != null && publishingDomains.length > 0) {
                try {
                    FrameworkUtils.startTenantFlow(MultitenantConstants.SUPER_TENANT_DOMAIN_NAME);
                    for (String publishingDomain : publishingDomains) {
                        Object[] metadataArray = AuthnDataPublisherUtils.getMetaDataArray(publishingDomain);

                        Event event = new Event(CUSTOM_STREAM_NAME, System.currentTimeMillis(), metadataArray, null,
                                payloadData);
                        SampleEventPublisherDataHolder.getInstance().getPublisherService().publish(event);
                        if (log.isDebugEnabled()) {
                            log.debug("Sending out event : " + event.toString());
                        }
                    }
                } finally {
                    FrameworkUtils.endTenantFlow();
                }
            }
        } catch (IdentityRuntimeException e) {
            log.error("Error while publishing authentication data", e);
        }
    }

    /**
     * Send the event as a HTTP Post request directly to the analytics server. In this sample we are sending the
     * request to http://webhook.site.
     *
     * @param authenticationData
     */
    private void publishEventUsingHttpClient(AuthenticationData authenticationData) {

        String baseURL = "http://webhook.site/f700d195-a959-4eb4-8812-6fc5f55a9f28";
        HttpPost httpPost = new HttpPost(baseURL);

        // Create the json body for the request.
        JSONObject eventProperties = new JSONObject();
        eventProperties.put("type", authenticationData.isAuthnSuccess() ? "SUCCESS" : "FAILURE");

        JSONObject userProperties = new JSONObject();
        userProperties.put("application", authenticationData.getServiceProvider());
        userProperties.put("username", authenticationData.getUsername());

        JSONObject obj = new JSONObject();

        obj.put("apiKey", "my_amplitude_api_key");
        obj.put("eventType", authenticationData.getEventType());
        obj.put("event_properties", eventProperties);
        obj.put("user_properties", userProperties);

        StringEntity userDataStringEntity = new StringEntity(obj.toString(), "UTF-8");

        httpPost.setEntity(userDataStringEntity);
        httpPost.setHeader("Content-type", "application/json");

        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();

        CloseableHttpClient httpClient = httpClientBuilder.build();
        try {
            httpClient.execute(httpPost);
        } catch (IOException e) {
            log.error("Error while publishing authentication data.", e);
        }
    }
}
