package org.apache.thrift.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TCachedHttpClient;

public class TCachedProtocol extends TProtocol {
	private TProtocol mProt;
	private TCachedHttpClient mClient;
	private TField mCurrentField = null;
	private List<String> mIgnoreFields = new ArrayList<String>();

	public TCachedProtocol(TProtocol t) {
		super(t.getTransport());
		if (!(t.getTransport() instanceof TCachedHttpClient))
			throw new RuntimeException(
					"TCachedProtocol must used with TCachedHttpClient");
		mClient = (TCachedHttpClient) t.getTransport();
		mProt = t;
	}

	public void addIgnoreFields(String s) {
		if (s != null)
			mIgnoreFields.add(s);
	}

	public void addIgnoreFields(Collection<String> s) {
		if (s != null)
			mIgnoreFields.addAll(s);
	}

	@Override
	public void writeMessageBegin(TMessage message) throws TException {
		mProt.writeMessageBegin(message);
		mClient.writeCacheKey("api=" + message.name + "&");
	}

	@Override
	public void writeMessageEnd() throws TException {
		mProt.writeMessageEnd();
	}

	@Override
	public void writeStructBegin(TStruct struct) throws TException {
		mProt.writeStructBegin(struct);
	}

	@Override
	public void writeStructEnd() throws TException {
		mProt.writeStructEnd();
	}

	@Override
	public void writeFieldBegin(TField field) throws TException {
		mProt.writeFieldBegin(field);
		mCurrentField = field;
		if (mIgnoreFields.contains(mCurrentField.name)) {
			mCurrentField = null;
		}
	}

	@Override
	public void writeFieldEnd() throws TException {
		mProt.writeFieldEnd();
		mCurrentField = null;
	}

	@Override
	public void writeFieldStop() throws TException {
		mProt.writeFieldStop();
	}

	@Override
	public void writeMapBegin(TMap map) throws TException {
		mProt.writeMapBegin(map);
	}

	@Override
	public void writeMapEnd() throws TException {
		mProt.writeMapEnd();
	}

	@Override
	public void writeListBegin(TList list) throws TException {
		mProt.writeListBegin(list);
	}

	@Override
	public void writeListEnd() throws TException {
		mProt.writeListEnd();
	}

	@Override
	public void writeSetBegin(TSet set) throws TException {
		mProt.writeSetBegin(set);
	}

	@Override
	public void writeSetEnd() throws TException {
		mProt.writeSetEnd();
	}

	@Override
	public void writeBool(boolean b) throws TException {
		mProt.writeBool(b);
		writeFieldCacheKey("" + b);
	}

	@Override
	public void writeByte(byte b) throws TException {
		mProt.writeByte(b);
		writeFieldCacheKey("" + b);
	}

	@Override
	public void writeI16(short i16) throws TException {
		mProt.writeI16(i16);
		writeFieldCacheKey("" + i16);
	}

	@Override
	public void writeI32(int i32) throws TException {
		mProt.writeI32(i32);
		writeFieldCacheKey("" + i32);
	}

	@Override
	public void writeI64(long i64) throws TException {
		mProt.writeI64(i64);
		writeFieldCacheKey("" + i64);
	}

	@Override
	public void writeDouble(double dub) throws TException {
		mProt.writeDouble(dub);
		writeFieldCacheKey("" + dub);
	}

	@Override
	public void writeString(String str) throws TException {
		mProt.writeString(str);
		writeFieldCacheKey(str);
	}

	private void writeFieldCacheKey(String value) {
		if (mCurrentField != null)
			mClient.writeCacheKey(mCurrentField.name + "=" + value + "&");
	}

	@Override
	public void writeBinary(ByteBuffer buf) throws TException {
		mProt.writeBinary(buf);
	}

	@Override
	public TMessage readMessageBegin() throws TException {
		return mProt.readMessageBegin();
	}

	@Override
	public void readMessageEnd() throws TException {
		mProt.readMessageEnd();
	}

	@Override
	public TStruct readStructBegin() throws TException {
		return mProt.readStructBegin();
	}

	@Override
	public void readStructEnd() throws TException {
		mProt.readStructEnd();
	}

	@Override
	public TField readFieldBegin() throws TException {
		return mProt.readFieldBegin();
	}

	@Override
	public void readFieldEnd() throws TException {
		mProt.readFieldEnd();
	}

	@Override
	public TMap readMapBegin() throws TException {
		return mProt.readMapBegin();
	}

	@Override
	public void readMapEnd() throws TException {
		mProt.readMapEnd();
	}

	@Override
	public TList readListBegin() throws TException {
		return mProt.readListBegin();
	}

	@Override
	public void readListEnd() throws TException {
		mProt.readListEnd();
	}

	@Override
	public TSet readSetBegin() throws TException {
		return mProt.readSetBegin();
	}

	@Override
	public void readSetEnd() throws TException {
		mProt.readSetEnd();
	}

	@Override
	public boolean readBool() throws TException {
		return mProt.readBool();
	}

	@Override
	public byte readByte() throws TException {
		return mProt.readByte();
	}

	@Override
	public short readI16() throws TException {
		return mProt.readI16();
	}

	@Override
	public int readI32() throws TException {
		return mProt.readI32();
	}

	@Override
	public long readI64() throws TException {
		return mProt.readI64();
	}

	@Override
	public double readDouble() throws TException {
		return mProt.readDouble();
	}

	@Override
	public String readString() throws TException {
		return mProt.readString();
	}

	@Override
	public ByteBuffer readBinary() throws TException {
		return mProt.readBinary();
	}
}
