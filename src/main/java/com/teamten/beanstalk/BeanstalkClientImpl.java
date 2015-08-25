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

import java.io.IOException;

import java.util.List;
import java.util.Map;

/**
 * Concrete implementation of the BeanstalkClient interface.
 */
public class BeanstalkClientImpl implements BeanstalkClient {
    private static final String CLIENT_VERSION = "1.4.8";
    private static final long MAX_PRIORITY = 4294967296L;
    private ProtocolHandler protocolHandler = null;

    /**
     * Create a client with the default {@link BeanstalkClient.DEFAULT_HOST host}
     * and {@link BeanstalkClient.DEFAULT_PORT port}.
     *
     * @throws IOException if it could not connect to the server.
     */
    public BeanstalkClientImpl() throws IOException {
        this(DEFAULT_HOST, DEFAULT_PORT);
    }

    /**
     * Create a client with the specified host and port.
     *
     * @throws IOException if it could not connect to the server.
     */
    public BeanstalkClientImpl(String host, int port) throws IOException {
        protocolHandler = new ProtocolHandler(host, port);
    }

    // ****************************************************************
    // Producer methods
    // ****************************************************************
    @Override // BeanstalkClient
    public long put(long priority, int delaySeconds, int timeToRun, byte[] data) throws IOException {
        if (data == null) {
            throw new NullPointerException("null data");
        }
        if (priority > MAX_PRIORITY) {
            throw new IllegalArgumentException("invalid priority");
        }
        long jobId = -1;
        Request request = new Request(
                "put " + priority + " " + delaySeconds + " " + timeToRun + " " + data.length,
                new String[] {
                    "INSERTED", "BURIED"
                },
                new String[] {
                    "JOB_TOO_BIG"
                },
                data,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        if (response != null && response.getStatus().equals("JOB_TOO_BIG")) {
            throw new BeanstalkException(response.getStatus());
        }
        if (response != null && response.isMatchOk()) {
            jobId = Long.parseLong(response.getReponse());
        }
        return jobId;
    }

    @Override // BeanstalkClient
    public void useTube(String tubeName) throws IOException {
        if (tubeName == null) {
            throw new NullPointerException("null tubeName");
        }
        Request request = new Request(
                "use " + tubeName,
                "USING",
                null,
                null,
                ExpectedResponse.None);
        protocolHandler.processRequest(request);
    }

    // ****************************************************************
    // Consumer methods
    //	job-related
    // ****************************************************************	
    @Override // BeanstalkClient
    public Job reserve(Integer timeoutSeconds) throws IOException {
        Job job = null;
        String command = (timeoutSeconds == null)
                         ? "reserve"
                         : "reserve-with-timeout " + timeoutSeconds.toString();
        Request request = new Request(
                command,
                new String[] {
                    "RESERVED"
                },
                new String[] {
                    "DEADLINE_SOON", "TIMED_OUT",
                },
                null,
                ExpectedResponse.ByteArray,
                2);
        Response response = protocolHandler.processRequest(request);
        if (response != null && response.getStatus().equals("DEADLINE_SOON")) {
            throw new BeanstalkException(response.getStatus());
        }
        if (response != null && response.isMatchOk()) {
            long jobId = Long.parseLong(response.getReponse());
            job = new JobImpl(jobId);
            job.setData((byte[]) response.getData());
        }
        return job;
    }

    @Override // BeanstalkClient
    public boolean delete(long jobId) throws IOException {
        Request request = new Request(
                "delete " + jobId,
                "DELETED",
                "NOT_FOUND",
                null,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        return response != null && response.isMatchOk();
    }

    @Override // BeanstalkClient
    public boolean release(long jobId, long priority, int delaySeconds) throws IOException {
        Request request = new Request(
                "release " + jobId + " " + priority + " " + delaySeconds,
                new String[] {
                    "RELEASED"
                },
                new String[] {
                    "NOT_FOUND", "BURIED"
                },
                null,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        return response != null && response.isMatchOk();
    }

    @Override // BeanstalkClient
    public boolean bury(long jobId, long priority) throws IOException {
        Request request = new Request(
                "bury " + jobId + " " + priority,
                "BURIED",
                "NOT_FOUND",
                null,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        return response != null && response.isMatchOk();
    }

    @Override // BeanstalkClient
    public boolean touch(long jobId) throws IOException {
        Request request = new Request(
                "touch " + jobId,
                "TOUCHED",
                "NOT_FOUND",
                null,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        return response != null && response.isMatchOk();
    }

    // ****************************************************************
    // Consumer methods
    //	tube-related
    // ****************************************************************
    @Override // BeanstalkClient
    public int watch(String tubeName) throws IOException {
        if (tubeName == null) {
            throw new NullPointerException("null tubeName");
        }
        Request request = new Request(
                "watch " + tubeName,
                "WATCHING",
                null,
                null,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        return Integer.parseInt(response.getReponse());
    }

    @Override // BeanstalkClient
    public int ignore(String tubeName) throws IOException {
        if (tubeName == null) {
            throw new NullPointerException("null tubeName");
        }
        Request request = new Request(
                "ignore " + tubeName,
                new String[] {
                    "WATCHING", "NOT_IGNORED"
                },
                null,
                null,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        return (response.getReponse() == null) ? -1 : Integer.parseInt(response.getReponse());
    }

    // ****************************************************************
    // Consumer methods
    //	peek-related
    // ****************************************************************
    @Override // BeanstalkClient
    public Job peek(long jobId) throws IOException {
        Job job = null;
        Request request = new Request(
                "peek " + jobId,
                "FOUND",
                "NOT_FOUND",
                null,
                ExpectedResponse.ByteArray,
                2);
        Response response = protocolHandler.processRequest(request);
        if (response != null && response.isMatchOk()) {
            jobId = Long.parseLong(response.getReponse());
            job = new JobImpl(jobId);
            job.setData((byte[]) response.getData());
        }
        return job;
    }

    @Override // BeanstalkClient
    public Job peekBuried() throws IOException {
        Job job = null;
        Request request = new Request(
                "peek-buried",
                "FOUND",
                "NOT_FOUND",
                null,
                ExpectedResponse.ByteArray,
                2);
        Response response = protocolHandler.processRequest(request);
        if (response != null && response.isMatchOk()) {
            long jobId = Long.parseLong(response.getReponse());
            job = new JobImpl(jobId);
            job.setData((byte[]) response.getData());
        }
        return job;
    }

    @Override // BeanstalkClient
    public Job peekDelayed() throws IOException {
        Job job = null;
        Request request = new Request(
                "peek-delayed",
                "FOUND",
                "NOT_FOUND",
                null,
                ExpectedResponse.ByteArray,
                2);
        Response response = protocolHandler.processRequest(request);
        if (response != null && response.isMatchOk()) {
            long jobId = Long.parseLong(response.getReponse());
            job = new JobImpl(jobId);
            job.setData((byte[]) response.getData());
        }
        return job;
    }

    @Override // BeanstalkClient
    public Job peekReady() throws IOException {
        Job job = null;
        Request request = new Request(
                "peek-ready",
                "FOUND",
                "NOT_FOUND",
                null,
                ExpectedResponse.ByteArray,
                2);
        Response response = protocolHandler.processRequest(request);
        if (response != null && response.isMatchOk()) {
            long jobId = Long.parseLong(response.getReponse());
            job = new JobImpl(jobId);
            job.setData((byte[]) response.getData());
        }
        return job;
    }

    @Override // BeanstalkClient
    public int kick(int count) throws IOException {
        Request request = new Request(
                "kick " + count,
                "KICKED",
                null,
                null,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        if (response != null && response.isMatchOk()) {
            count = Integer.parseInt(response.getReponse());
        }
        return count;
    }

    // ****************************************************************
    // Consumer methods
    //	stats-related
    // ****************************************************************
    @SuppressWarnings("unchecked")
    @Override // BeanstalkClient
    public Map<String, String> statsJob(long jobId) throws IOException {
        Request request = new Request(
                "stats-job " + jobId,
                "OK",
                "NOT_FOUND",
                null,
                ExpectedResponse.Map);
        Response response = protocolHandler.processRequest(request);
        Map<String, String> map = null;
        if (response != null && response.isMatchOk()) {
            map = (Map<String, String>) response.getData();
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    @Override // BeanstalkClient
    public Map<String, String> statsTube(String tubeName) throws IOException {
        if (tubeName == null) {
            return null;
        }

        Request request = new Request(
                "stats-tube " + tubeName,
                "OK",
                "NOT_FOUND",
                null,
                ExpectedResponse.Map);
        Response response = protocolHandler.processRequest(request);
        Map<String, String> map = null;
        if (response != null && response.isMatchOk()) {
            map = (Map<String, String>) response.getData();
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    @Override // BeanstalkClient
    public Map<String, String> stats() throws IOException {
        Request request = new Request(
                "stats",
                "OK",
                null,
                null,
                ExpectedResponse.Map);
        Response response = protocolHandler.processRequest(request);
        Map<String, String> map = null;
        if (response != null && response.isMatchOk()) {
            map = (Map<String, String>) response.getData();
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    @Override // BeanstalkClient
    public List<String> listTubes() throws IOException {
        Request request = new Request(
                "list-tubes",
                "OK",
                null,
                null,
                ExpectedResponse.List);
        Response response = protocolHandler.processRequest(request);
        List<String> list = null;
        if (response != null && response.isMatchOk()) {
            list = (List<String>) response.getData();
        }
        return list;
    }

    @Override // BeanstalkClient
    public String listTubeUsed() throws IOException {
        String tubeName = null;
        Request request = new Request(
                "list-tube-used",
                "USING",
                null,
                null,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        if (response != null && response.isMatchOk()) {
            tubeName = response.getReponse();
        }
        return tubeName;
    }

    @SuppressWarnings("unchecked")
    @Override // BeanstalkClient
    public List<String> listTubesWatched() throws IOException {
        Request request = new Request(
                "list-tubes-watched",
                "OK",
                null,
                null,
                ExpectedResponse.List);
        Response response = protocolHandler.processRequest(request);
        List<String> list = null;
        if (response != null && response.isMatchOk()) {
            list = (List<String>) response.getData();
        }
        return list;
    }

    @Override // BeanstalkClient
    public String getClientVersion() {
        return CLIENT_VERSION;
    }

    @Override // BeanstalkClient
    public void close() {
        protocolHandler.close();
    }

    @Override // BeanstalkClient
    public boolean pauseTube(String tubeName, int pauseDelay) throws IOException {
        Request request = new Request(
                "pause-tube " + tubeName + " " + pauseDelay,
                "PAUSED",
                null,
                null,
                ExpectedResponse.None);
        Response response = protocolHandler.processRequest(request);
        if (response != null && response.isMatchOk()) {
            return true;
        }
        return false;
    }

    @Override // BeanstalkClient
    public String getServerVersion() throws IOException {
        Map<String, String> stats = stats();
        if (stats == null) {
            throw new BeanstalkException("could not get stats");
        }
        return stats.get("version").trim();
    }
}
