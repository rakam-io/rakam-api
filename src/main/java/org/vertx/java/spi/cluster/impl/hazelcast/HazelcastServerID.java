/*
 * Copyright (c) 2011-2013 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 *     The Eclipse Public License is available at
 *     http://www.eclipse.org/legal/epl-v10.html
 *
 *     The Apache License v2.0 is available at
 *     http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package org.vertx.java.spi.cluster.impl.hazelcast;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.vertx.java.core.net.impl.ServerID;

import java.io.IOException;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
/*
 * I used IdentifiedDataSerializable in order to prevent from reflection,
 * Hazelcast identifies the class by matching factoryId values.
 * @author <http://github.com/buremba> Burak Emre Kabakcı
 */
class HazelcastServerID extends ServerID implements IdentifiedDataSerializable {

  public HazelcastServerID() {
  }

  public HazelcastServerID(ServerID serverID) {
    super(serverID.port, serverID.host);
  }

  // We replace any ServerID instances with HazelcastServerID - this allows them to be serialized more optimally using
  // DataSerializable
  public static <V> V convertServerID(V val) {
    if (val.getClass() == ServerID.class) {
      ServerID sid = (ServerID)val;
      HazelcastServerID hsid = new HazelcastServerID(sid);
      return (V)hsid;
    } else {
      return val;
    }
  }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(port);
        out.writeUTF(host);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        port = in.readInt();
        host = in.readUTF();
    }

    @Override
    public int getFactoryId() {
        return 1;
    }

    @Override
    public int getId() {
        return 0;
    }
}
