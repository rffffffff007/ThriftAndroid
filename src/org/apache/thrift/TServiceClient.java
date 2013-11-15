/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift;

import java.lang.reflect.Method;

import org.apache.thrift.protocol.TCachedProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TCachedHttpClient;
import org.apache.thrift.transport.TTransport;

import android.util.Log;

/**
 * A TServiceClient is used to communicate with a TService implementation
 * across protocols and transports.
 */
public abstract class TServiceClient {
	private static final String TAG = "TServiceClient";

	public TServiceClient(TProtocol prot) {
		this(prot, prot);
	}

	public TServiceClient(TProtocol iprot, TProtocol oprot) {
		iprot_ = iprot;
		oprot_ = oprot;
	}

	protected TProtocol iprot_;
	protected TProtocol oprot_;

	protected int seqid_;

	/**
	 * Get the TProtocol being used as the input (read) protocol.
	 * 
	 * @return the TProtocol being used as the input (read) protocol.
	 */
	public TProtocol getInputProtocol() {
		return this.iprot_;
	}

	/**
	 * Get the TProtocol being used as the output (write) protocol.
	 * 
	 * @return the TProtocol being used as the output (write) protocol.
	 */
	public TProtocol getOutputProtocol() {
		return this.oprot_;
	}

	protected void sendBase(String methodName, TBase args) throws TException {
		// added by renfei
		Log.i(TAG, methodName + ": " + args.toString());
		oprot_.writeMessageBegin(new TMessage(methodName, TMessageType.CALL,
				++seqid_));
		args.write(oprot_);
		oprot_.writeMessageEnd();
		oprot_.getTransport().flush();
	}

	protected void receiveBase(TBase result, String methodName)
			throws TException {
		TMessage msg = iprot_.readMessageBegin();
		if (msg.type == TMessageType.EXCEPTION) {
			TApplicationException x = TApplicationException.read(iprot_);
			iprot_.readMessageEnd();
			throw x;
		}
		// added by renfei
		// if the protocol is TCachedProtocol, the seqid do not need to verify.
		if (!(getInputProtocol() instanceof TCachedProtocol)) {
			if (msg.seqid != seqid_) {
				throw new TApplicationException(
						TApplicationException.BAD_SEQUENCE_ID, methodName
								+ " failed: out of sequence response");
			}
		}

		result.read(iprot_);
		iprot_.readMessageEnd();

		// added by renfei
		// if return is not success, then clear the cache if the Transport is TCachedHttpClient.
		try {
			TTransport transport = getInputProtocol().getTransport();
			if (transport != null && transport instanceof TCachedHttpClient) {
				TFieldIdEnum fieldSuccess = result.fieldForId(0);
				boolean isSuccess = result.isSet(fieldSuccess);
				if (!isSuccess) {
					((TCachedHttpClient) transport).clearCachedData();
				}
			}
		} catch (Exception e) {
			Log.e(TAG, "", e);
		}

		String resultStr = result.toString();
		int size = resultStr.length();
		if (size > 200)
			resultStr = resultStr.substring(0, 200);
		// added by renfei
		Log.i(TAG, methodName + " " + size + ": " + resultStr);
	}

}
