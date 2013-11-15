package org.apache.thrift.transport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.client.HttpClient;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import android.content.Context;
import android.util.Log;
import cn.buding.common.util.CodecUtils;
import cn.buding.common.util.Hex;
import cn.buding.common.util.NTPTime;
import cn.buding.common.util.PreferenceHelper;

/**
 * added by renfei.
 */
public class TCachedHttpClient extends THttpClient {
	private static final String TAG = "TCachedHttpClient";
	/**
	 * if the cache time is in available time, the cache will be returned
	 * directly, the api will not be requested.
	 */
	private long mCacheAvailableTime = 3 * 3600 * 1000;

	/**
	 * if the cache time is in mCacheValidTime, the cache will be returned the
	 * after the http request failed.
	 */
	private long mCacheValidTime = 2 * 24 * 3600 * 1000;

	protected final java.io.CharArrayWriter cacheKeyBuffer = new CharArrayWriter();

	private final Context context;

	public static String DEFAULT_PREFERENCE_NAME_PROPERTYARRAY = "preference_thrift_cahce";

	private PreferenceHelper pHelper;

	public static final int FLAG_CLEAR_CACHE = 0x1;
	public static final int FLAG_SKIP_CACHE = 0x2;
	private int mFlag;

	public TCachedHttpClient(Context context, String url, HttpClient client)
			throws TTransportException {
		super(url, client);
		if (null == client) {
			throw new TTransportException("Null HttpClient, aborting.");
		}
		this.context = context;
		pHelper = PreferenceHelper.getHelper(context,
				DEFAULT_PREFERENCE_NAME_PROPERTYARRAY);
	}

	public void setClearCache(boolean b) {
		if (b) {
			addFlag(FLAG_CLEAR_CACHE);
		} else {
			removeFlag(FLAG_CLEAR_CACHE);
		}
	}

	public boolean isClearCacheMode() {
		return (mFlag & FLAG_CLEAR_CACHE) != 0;
	}

	public boolean isSkipCacheMode() {
		return (mFlag & FLAG_SKIP_CACHE) != 0;
	}

	public void addFlag(int flag) {
		mFlag |= flag;
	}

	public void removeFlag(int flag) {
		mFlag &= ~flag;
	}

	public void setCacheAvailableTime(long time) {
		mCacheAvailableTime = time;
	}

	public void setCacheValidTime(long time) {
		mCacheValidTime = time;
	}

	public void write(byte[] buf, int off, int len) {
		// write() or writeCahceKey() will change the cacheKey. so assign the
		// old cacheKey as null.
		mCacheKey = null;
		super.write(buf, off, len);
	}

	public void writeCacheKey(String s) {
		mCacheKey = null;
		cacheKeyBuffer.append(s);
	}

	private String mCacheKey = null;

	protected String getCacheKey() {
		if (mCacheKey == null) {
			String param = "";
			if (cacheKeyBuffer.size() == 0) {
				byte[] data = requestBuffer_.toByteArray();
				param = new String(Base64.encodeBase64(data));
			} else {
				param = cacheKeyBuffer.toString();
			}
			param = CodecUtils.md5Hex(param);
			mCacheKey = url_.toString() + "?" + param;
		}
		return mCacheKey;
	}

	public void clearCachedData() {
		String key = getCacheKey();
		pHelper.removePreferenceWithDate(key);
		Log.i(TAG, "Clear cache:" + key);
	}

	private boolean isCacheTimeAvail(long cacheTime) {
		return NTPTime.currentTimeMillis() - cacheTime < mCacheAvailableTime;
	}

	private boolean isCacheTimeValid(long cacheTime) {
		return NTPTime.currentTimeMillis() - cacheTime < mCacheValidTime;
	}

	private boolean checkCacheFilter(String key, String cache) {
		return mFilter == null || mFilter.filter(key, cache);
	}

	public void flush() throws TTransportException {
		String key = getCacheKey();
		// If ClearCacheMode = true, we just clear the corresponding cache to
		// this url.
		if (isClearCacheMode()) {
			clearCachedData();
			return;
		}
		mLoadFromCache = false;

		Date cacheDate = new Date();
		String cache = pHelper.readPreferenceAndDate(key, cacheDate);
		long cacheTime = cacheDate.getTime();

		/*
		 * 1 return data from cache if cache time is available, otherwise return
		 * data from http request.
		 * 
		 * 2 if http request fails, still return cache if cache time is valid.
		 */
		if (cache != null && isCacheTimeAvail(cacheTime)
				&& checkCacheFilter(key, cache) && !isSkipCacheMode()) {
			Log.i(TAG, "From cache");
			flushByCache(cache);
			restoreHeader(key);
		} else {
			Log.i(TAG, "From http");
			try {
				super.flush();
				saveCache(key);
			} catch (TTransportException t) {
				if (cache != null && isCacheTimeValid(cacheTime)
						&& checkCacheFilter(key, cache)) {
					Log.i(TAG, "From http failed, still return cache: " + key);
					flushByCache(cache);
					restoreHeader(key);
				}
			}
		}
		cacheKeyBuffer.reset();
	}

	private void saveCache(String key) {
		if (key == null)
			return;
		try {
			byte[] responseData = readInputStream(inputStream_);
			inputStream_ = new ByteArrayInputStream(responseData);
			String response = new String(Base64.encodeBase64(responseData));
			pHelper.writePreferenceWithDate(key, response);

			saveHeader(key);
		} catch (Exception e) {
			Log.e(TAG, "", e);
		}
	}

	private void saveHeader(String key) {
		String headerKey = getHeaderCacheKey(key);
		String header = "{}";
		try {
			if (mResponseHeaders != null) {
				JSONObject job = new JSONObject();
				for (String name : mResponseHeaders.keySet()) {
					List<String> values = mResponseHeaders.get(name);
					if (values != null) {
						job.put(name, new JSONArray(values));
					}
				}
				header = job.toString();
			}
		} catch (Exception e) {
			Log.e(TAG, "", e);
		}
		pHelper.writePreference(headerKey, header);
	}

	private void restoreHeader(String key) {
		String headerKey = getHeaderCacheKey(key);
		String header = pHelper.readPreference(headerKey);
		try {
			JSONObject job = new JSONObject(header);
			JSONArray names = job.names();
			Map<String, List<String>> maps = new HashMap<String, List<String>>();
			if (names != null) {
				for (int i = 0; i < names.length(); i++) {
					String name = names.getString(i);
					JSONArray array = new JSONArray(job.getString(name));
					List<String> value = parseToList(array);
					maps.put(name, value);
				}
			}
			mResponseHeaders = maps;
		} catch (Exception e) {
			Log.e(TAG, "", e);
		}
	}

	private List<String> parseToList(JSONArray valueArray) throws JSONException {
		if (valueArray == null)
			return null;
		List<String> valueList = new ArrayList<String>();
		for (int j = 0; j < valueArray.length(); j++) {
			valueList.add(valueArray.getString(j));
		}
		return valueList;
	}

	private String getHeaderCacheKey(String key) {
		return key + "_header";
	}

	private boolean mLoadFromCache = false;

	public boolean isLoadFromCache() {
		return mLoadFromCache;
	}

	private byte[] readInputStream(InputStream is) throws IOException {
		try {
			byte[] buf = new byte[1024];
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			int len = 0;
			while ((len = is.read(buf)) > 0) {
				baos.write(buf, 0, len);
			}
			return baos.toByteArray();
		} finally {
			is.close();
		}
	}

	private void flushByCache(String cache) {
		mLoadFromCache = true;
		byte[] data = Base64.decodeBase64(cache.getBytes());
		inputStream_ = new ByteArrayInputStream(data);
	}

	private TCacheFilter mFilter;

	public void setCacheFilter(TCacheFilter filter) {
		mFilter = filter;
	}

	public interface TCacheFilter {
		public boolean filter(String cacheKey, String cacheData);

	}
}
