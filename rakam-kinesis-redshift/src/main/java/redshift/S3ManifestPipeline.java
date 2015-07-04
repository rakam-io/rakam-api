/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
package redshift;

import com.amazonaws.services.kinesis.connectors.KinesisConnectorConfiguration;
import com.amazonaws.services.kinesis.connectors.impl.AllPassFilter;
import com.amazonaws.services.kinesis.connectors.impl.BasicMemoryBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IBuffer;
import com.amazonaws.services.kinesis.connectors.interfaces.IEmitter;
import com.amazonaws.services.kinesis.connectors.interfaces.IFilter;
import com.amazonaws.services.kinesis.connectors.interfaces.IKinesisConnectorPipeline;
import com.amazonaws.services.kinesis.connectors.interfaces.ITransformer;
import org.rakam.analysis.S3ManifestEmitter;
import org.rakam.collection.Event;
import org.rakam.collection.event.metastore.Metastore;

public class S3ManifestPipeline implements IKinesisConnectorPipeline<Event, byte[]> {

    private final Metastore metastore;

    public S3ManifestPipeline(Metastore metastore) {
        this.metastore = metastore;
    }

    @Override
    public IEmitter<byte[]> getEmitter(KinesisConnectorConfiguration configuration) {
        return new S3ManifestEmitter(configuration);
    }

    @Override
    public IBuffer<Event> getBuffer(KinesisConnectorConfiguration configuration) {
        return new BasicMemoryBuffer<>(configuration);
    }

    @Override
    public ITransformer<Event, byte[]> getTransformer(KinesisConnectorConfiguration configuration) {
        return new KinesisMessageModelRedshiftTransformer(metastore, configuration);
    }

    @Override
    public IFilter<Event> getFilter(KinesisConnectorConfiguration configuration) {
        return new AllPassFilter();
    }

}
