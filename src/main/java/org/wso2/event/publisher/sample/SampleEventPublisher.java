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
import org.wso2.carbon.base.MultitenantConstants;
import org.wso2.carbon.identity.application.authentication.framework.util.FrameworkUtils;
import org.wso2.carbon.identity.base.IdentityRuntimeException;
import org.wso2.carbon.identity.data.publisher.application.authentication.AuthPublisherConstants;
import org.wso2.carbon.identity.data.publisher.application.authentication.AuthnDataPublisherUtils;
import org.wso2.carbon.identity.data.publisher.authentication.analytics.login.AnalyticsLoginDataPublishConstants;
import org.wso2.carbon.identity.data.publisher.authentication.analytics.login.AnalyticsLoginDataPublisherUtils;
import org.wso2.carbon.identity.data.publisher.authentication.analytics.login.model.AuthenticationData;
import org.wso2.carbon.identity.event.IdentityEventConstants;
import org.wso2.carbon.identity.event.IdentityEventException;
import org.wso2.carbon.identity.event.event.Event;
import org.wso2.carbon.identity.event.handler.AbstractEventHandler;
import org.wso2.event.publisher.sample.internal.SampleEventPublisherDataHolder;

public class SampleEventPublisher extends AbstractEventHandler {

    private static final String CUSTOM_STREAM_NAME = "org.wso2.is.analytics.stream.CustomAuthData:1.0.0";
    private static final String CUSTOM_LOGIN_DATA_PUBLISHER_ENABLED = "customLoginDataPublisher.enable";
    private static final String CUSTOM_EVENT_PUBLISHER = "customLoginDataPublisher";
    private static Log log = LogFactory.getLog(SampleEventPublisher.class);

    @Override
    public String getName() {

        return CUSTOM_EVENT_PUBLISHER;
    }

    @Override
    public void handleEvent(Event event) throws IdentityEventException {

        // Check whether the event publishing is enabled. If not, return without publishing the event to the external
        // server.
        if (!isCustomLoginDataPublishingEnabled()) {
            return;
        }

        if (IdentityEventConstants.EventName.AUTHENTICATION_STEP_SUCCESS.name().equals(event.getEventName()) ||
                IdentityEventConstants.EventName.AUTHENTICATION_STEP_FAILURE.name().equals(event.getEventName())) {
            AuthenticationData authenticationData = AnalyticsLoginDataPublisherUtils.buildAuthnDataForAuthnStep(event);
            publishAuthenticationData(authenticationData);
        } else if (IdentityEventConstants.EventName.AUTHENTICATION_SUCCESS.name().equals(event.getEventName()) ||
                IdentityEventConstants.EventName.AUTHENTICATION_FAILURE.name().equals(event.getEventName())) {
            AuthenticationData authenticationData = AnalyticsLoginDataPublisherUtils.
                    buildAuthnDataForAuthentication(event);
            publishAuthenticationData(authenticationData);
        } else {
            log.error("Event " + event.getEventName() + " cannot be handled");
        }
    }

    /**
     * Create the payload and publish the event to the adapter.
     *
     * @param authenticationData authentication data
     */
    private void publishAuthenticationData(AuthenticationData authenticationData) {

        Object[] payload = createPayload(authenticationData);
        publishToAnalytics(authenticationData, payload);
    }

    /**
     * This method will construct the payload to be published from the authentication data.
     * In this sample we will publish the authentication result, username, userstore domain, tenant domain,
     * application name and authenticator details.
     *
     * @param authenticationData authentication data
     * @return payload to be published
     */
    private Object[] createPayload(AuthenticationData authenticationData) {

        Object[] payloadData = new Object[8];
        try {

            // Populating the event attributes
            payloadData[0] = "my_sample_key";
            payloadData[1] = authenticationData.getEventType();
            payloadData[2] = authenticationData.isAuthnSuccess();
            payloadData[3] = AuthnDataPublisherUtils.replaceIfNotAvailable(AuthPublisherConstants.CONFIG_PREFIX +
                    AuthPublisherConstants.USERNAME, authenticationData.getUsername());
            payloadData[4] = AuthnDataPublisherUtils.replaceIfNotAvailable(AuthPublisherConstants.CONFIG_PREFIX +
                    AuthPublisherConstants.USER_STORE_DOMAIN, authenticationData.getUserStoreDomain());
            payloadData[5] = authenticationData.getTenantDomain();
            payloadData[6] = AuthnDataPublisherUtils.replaceIfNotAvailable(AuthPublisherConstants.CONFIG_PREFIX +
                    AuthPublisherConstants.SERVICE_PROVIDER, authenticationData.getServiceProvider());
            payloadData[7] = AuthnDataPublisherUtils.replaceIfNotAvailable(AuthPublisherConstants.CONFIG_PREFIX +
                    AuthPublisherConstants.IDENTITY_PROVIDER, authenticationData.getIdentityProvider());
        } catch (IdentityRuntimeException e) {
            log.error("Error while publishing authentication data", e);
        }
        return payloadData;
    }

    /**
     * Check whether the data publishing is enabled. This configuration should be added in the identity-event
     * .properties file which resides inside {IS-HOME}/repository/conf/identity folder. The config for this publisher
     * will be as "customLoginDataPublisher.enable"
     * @return whether the publisher is enabled or not.
     */
    private boolean isCustomLoginDataPublishingEnabled() {

        if (this.configs.getModuleProperties() != null) {
            String handlerEnabled = this.configs.getModuleProperties()
                    .getProperty(CUSTOM_LOGIN_DATA_PUBLISHER_ENABLED);
            return Boolean.parseBoolean(handlerEnabled);
        }

        return false;
    }

    /**
     * This method will publish the created payload to the analytics component which will handle the event publishing
     * to the external analytics server.
     * In this sample, the event is published to a stream named, org.wso2.is.analytics.stream.CustomAuthData:1.0.0
     *
     * @param authenticationData authentication Data
     * @param payloadData        event payload to be published
     */
    private void publishToAnalytics(AuthenticationData authenticationData, Object[] payloadData) {
        // Publishing the event using the wso2 event adapters.
        String[] publishingDomains = (String[]) authenticationData.getParameter(AnalyticsLoginDataPublishConstants.TENANT_DOMAIN_NAMES);
        if (publishingDomains != null && publishingDomains.length > 0) {
            try {
                FrameworkUtils.startTenantFlow(MultitenantConstants.SUPER_TENANT_DOMAIN_NAME);
                for (String publishingDomain : publishingDomains) {
                    Object[] metadataArray = AuthnDataPublisherUtils.getMetaDataArray(publishingDomain);

                    org.wso2.carbon.databridge.commons.Event event = new org.wso2.carbon.databridge.commons
                            .Event(CUSTOM_STREAM_NAME, System.currentTimeMillis(), metadataArray, null,
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
    }
}
