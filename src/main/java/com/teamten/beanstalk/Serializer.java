package com.teamten.beanstalk;

/*
 *
 * Copyright 2009-2010 Robert Tykulsker *
 * This file is part of JavaBeanstalkCLient.
 *
 * JavaBeanstalkCLient is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version, or alternatively, the BSD license
 * supplied
 * with this project in the file "BSD-LICENSE".
 *
 * JavaBeanstalkCLient is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JavaBeanstalkCLient. If not, see <http://www.gnu.org/licenses/>.
 *
 */

import com.teamten.beanstalk.BeanstalkException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Utility classes for serializing Serializable objects to byte arrays, for putting
 * into and getting out of jobs.
 */
public class Serializer {
    /**
     * Serialize an object to a byte array using Java's standard serialization scheme.
     *
     * @param serializable the object to serialize.
     * @return the raw serialized array.
     * @throws IOException on any IO error.
     */
    public static byte[] serializableToByteArray(Serializable serializable) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(serializable);
        oos.flush();
        byte[] bytes = baos.toByteArray();
        oos.close();
        baos.close();

        return bytes;
    }

    /**
     * Deserialize a byte array into an object.
     *
     * @param bytes the raw serialized object.
     * @return the object that was serialized.
     * @throws IOException on any IO error.
     * @throws ClassNotFoundException if the object's class cannot be loaded.
     */
    public static Serializable byteArrayToSerializable(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);

        return (Serializable) ois.readObject();
    }
}
