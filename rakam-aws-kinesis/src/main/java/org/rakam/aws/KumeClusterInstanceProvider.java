/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rakam.aws;

import com.google.common.collect.ImmutableList;
import com.google.inject.Provider;
import org.rakam.kume.Cluster;
import org.rakam.kume.ClusterBuilder;
import org.rakam.kume.service.ServiceListBuilder;
import org.rakam.report.PrestoConfig;

import javax.inject.Inject;

/**
 * Created by buremba <Burak Emre Kabakcı> on 30/09/15 14:34.
 */
class KumeClusterInstanceProvider implements Provider<Cluster> {

    private final AWSKinesisModule.KumeConfig config;
    private final PrestoConfig prestoConfig;

    @Inject
    public KumeClusterInstanceProvider(PrestoConfig prestoConfig, AWSKinesisModule.KumeConfig config) {
        this.prestoConfig = prestoConfig;
        this.config = config;
    }

    @Override
    public Cluster get() {
        ImmutableList<ServiceListBuilder.Constructor> services = new ServiceListBuilder()
                .add("eventStreamer", KinesisEventStream::new)
                .build();
        return new ClusterBuilder()
                .services(services)
                .joinStrategy(new AWSKinesisModule.KumeJoinerService(prestoConfig, config))
                .serverAddress("0.0.0.0", 0)
                .client(true)
                .start();
    }
}
