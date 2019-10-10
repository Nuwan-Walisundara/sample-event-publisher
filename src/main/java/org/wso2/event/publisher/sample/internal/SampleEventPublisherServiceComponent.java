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

package org.wso2.event.publisher.sample.internal;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.wso2.carbon.event.stream.core.EventStreamService;
import org.wso2.carbon.identity.event.handler.AbstractEventHandler;
import org.wso2.event.publisher.sample.SampleEventPublisher;

/**
 * Registers the publisher as an osgi component.
 */
@Component(
        name = "sample.event.publisher",
        immediate = true
)
public class SampleEventPublisherServiceComponent {

    private static Log log = LogFactory.getLog(SampleEventPublisherServiceComponent.class);

    @Activate
    protected void activate(ComponentContext context) {
        if (log.isDebugEnabled()) {
            log.debug("Activating SampleEventPublisherServiceComponent");
        }
        try {
            context.getBundleContext().registerService(AbstractEventHandler.class.getName(),
                    new SampleEventPublisher(), null);
            if (log.isDebugEnabled()) {
                log.debug("SampleEventPublisherServiceComponent is activated");
            }
        } catch (Throwable e) {
            log.error("Error while activating SampleEventPublisherServiceComponent ", e);
        }
    }

    @Reference(
            name = "event.stream.service",
            service = EventStreamService.class,
            cardinality = ReferenceCardinality.MANDATORY,
            policy = ReferencePolicy.DYNAMIC,
            unbind = "unsetEventStreamService"
    )
    protected void setEventStreamService(EventStreamService publisherService) {

        SampleEventPublisherDataHolder.getInstance().setPublisherService(publisherService);
    }

    protected void unsetEventStreamService(EventStreamService publisherService) {

        SampleEventPublisherDataHolder.getInstance().setPublisherService(null);
    }
}
