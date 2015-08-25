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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.OutputStream;

import java.net.Socket;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Communicates with the server.
 */
class ProtocolHandler {
    private static final byte[] CRLF = {
        '\r', '\n'
    };
    private Socket socket;

    ProtocolHandler(String host, int port) throws IOException {
        socket = new Socket(host, port);
    }

    /**
     * Send the request to the server and return its response.
     */
    Response processRequest(Request request) throws IOException {
        validateRequest(request);

        Response response = null;
        InputStream is = null;
        OutputStream os = null;

        // formulate the request ...
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(request.getCommand().getBytes());
        baos.write(CRLF);
        if(request.getData() != null) {
            baos.write(request.getData());
            baos.write(CRLF);
        }
        baos.flush();
        os = socket.getOutputStream();
        os.write(baos.toByteArray());
        os.flush();
        baos.close();

        is = socket.getInputStream();
        String line = new String(readInputStream(is, 0));

        String[] tokens = line.split(" ");
        if(tokens == null || tokens.length == 0) {
            throw new BeanstalkException("no response");
        }

        response = new Response();
        response.setResponseLine(line);
        String status = tokens[0];
        response.setStatus(status);
        if(tokens.length > 1) {
            response.setReponse(tokens[1]);
        }
        setState(request, response, status);

        switch(request.getExpectedResponse()) {
            case Map:
                if(response.isMatchError()) {
                    break;
                }
                response.setData(parseForMap(is));
                break;
            case List:
                response.setData(parseForList(is));
                break;
            case ByteArray:
                if(response.isMatchError()) {
                    break;
                }
                int length;
                if(request.getExpectedDataLengthIndex() > 0) {
                    if (request.getExpectedDataLengthIndex() >= tokens.length) {
                        throw new BeanstalkException("length missing from response line");
                    }
                    String lengthStr = tokens[request.getExpectedDataLengthIndex()];
                    try {
                        length = Integer.parseInt(lengthStr);
                    } catch(NumberFormatException ex) {
                        throw new BeanstalkException("could not parse response length \"" + lengthStr + "\"");
                    }
                } else {
                    length = 0;
                }
                byte[] data = readInputStream(is, length);
                response.setData(data);
                break;
            default:
                break;
        }
        return response;
    }

    private byte[] readInputStream(InputStream is, int expectedLength) throws IOException {
        if(is == null) {
            return null;
        }

        byte[] data;

        if(expectedLength > 0) {
            data = readInputStreamBurstMode(is, expectedLength);
        } else {
            data = readInputStreamSlowMode(is);
        }
        return data;
    }

    private byte[] readInputStreamBurstMode(InputStream is, int length) throws IOException {
        byte[] data = new byte[length];
        // changes per alaz
        int off = 0;
        int toRead = length;
        while(toRead > 0) {
            int readLength = is.read(data, off, toRead);
            if(readLength == -1) {
                throw new BeanstalkException(String.format("The end of InputStream is reached - %d bytes expected, %d bytes read", length, off + readLength));
            }
            off += readLength;
            toRead -= readLength;
        }
        byte br = (byte) is.read();
        byte bn = (byte) is.read();
        if(br != '\r' || bn != '\n') {
            throw new BeanstalkException("The end of InputStream is reached - End of line expected, but not found");
        }
        return data;
    }

    /**
     * This routine reads from the input stream until a \r\n pair is found.
     * This must only be used for text lines in the protocol. Do you use this
     * for job data, since that would prevent binary data from being sent.
     */
    private byte[] readInputStreamSlowMode(InputStream is) throws IOException {
        boolean lastByteWasReturnByte = false;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        while(true) {
            int intB = is.read();
            byte b = (byte) intB;

            /**
             * prevent OutOfMemory exceptions, per leopoldkot
             */
            if(intB == -1) {
                throw new BeanstalkException("The end of InputStream is reached");
            }

            if (lastByteWasReturnByte) {
                if (b == '\n') {
                    // End of line.
                    break;
                }

                // Was lone \r.
                lastByteWasReturnByte = false;
                baos.write('\r');
            }
            if(b == '\r') {
                lastByteWasReturnByte = true;
            } else {
                baos.write(b);
            }
        }

        return baos.toByteArray();
    }

    /**
     * Make sure the request is okay before processing it.
     *
     * @throws NullPointerException if the request is null.
     * @throws IllegalArgumentException if the contents of the request are not valid.
     */
    private void validateRequest(Request request) {
        if(request == null) {
            throw new NullPointerException("null request");
        }

        String command = request.getCommand();
        if(command == null || command.length() == 0) {
            throw new IllegalArgumentException("null or empty command");
        }

        String[] validStates = request.getValidStates();
        if(validStates == null || validStates.length == 0) {
            throw new IllegalArgumentException("null or empty validStates");
        }
    }

    private void setState(Request request, Response response, String status) throws BeanstalkException {
        for(String s : request.getValidStates()) {
            if(status.equals(s)) {
                response.setMatchOk(true);
                break;
            }
        }

        if(!response.isMatchOk() && request.getErrorStates() != null) {
            for(String s : request.getErrorStates()) {
                if(status.equals(s)) {
                    response.setMatchError(true);
                    break;
                }
            }
        }

        if(!response.isMatchOk() && !response.isMatchError()) {
            throw new BeanstalkException(status);
        }
    }

    /**
     * Parse a YAML map.
     */
    private Map<String, String> parseForMap(InputStream is) throws IOException {
        Map<String, String> map = new LinkedHashMap<String, String>();
        String line = null;

        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        while((line = in.readLine()) != null) {
            if(line.length() == 0) {
                break;
            }
            String[] values = line.split(": ");
            if(values.length != 2) {
                continue;
            }
            map.put(values[0], values[1]);
        }

        return map;
    }

    /**
     * Parse a YAML list of string.
     */
    private List<String> parseForList(InputStream is) throws IOException {
        List<String> list = new ArrayList<String>();
        String line = null;

        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        while((line = in.readLine()) != null) {
            if(line.length() == 0) {
                break;
            }
            if(line.equals("---")) {
                continue;
            }
            list.add(line.substring(2));
        }

        return list;
    }

    public void close() {
        if(socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch(Exception e) {
                // Swallow exception closing the socket.
            }
        }
    }
}
