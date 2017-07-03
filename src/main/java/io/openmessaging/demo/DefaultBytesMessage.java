package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

import java.io.Serializable;
import java.util.Set;

public class DefaultBytesMessage implements BytesMessage, Serializable {

    private KeyValue headers = new DefaultKeyValue();
    private KeyValue properties;
    private byte[] body;

    public DefaultBytesMessage(byte[] body) {
        this.body = body;
    }

    @Override
    public byte[] getBody() {
        return body;
    }

    @Override
    public BytesMessage setBody(byte[] body) {
        this.body = body;
        return this;
    }

    @Override
    public KeyValue headers() {
        return headers;
    }

    @Override
    public KeyValue properties() {
        return properties;
    }

    @Override
    public Message putHeaders(String key, int value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, long value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, double value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putHeaders(String key, String value) {
        headers.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, int value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, long value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, double value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public Message putProperties(String key, String value) {
        if (properties == null) properties = new DefaultKeyValue();
        properties.put(key, value);
        return this;
    }

    @Override
    public boolean equals(Object obj2) {
        Object obj1 = this;
        if (!(obj1 instanceof DefaultBytesMessage) || !(obj2 instanceof DefaultBytesMessage)) {
            return false;
        }

        DefaultBytesMessage t = (DefaultBytesMessage) obj1;
        DefaultBytesMessage m = (DefaultBytesMessage) obj2;

        // header
        DefaultKeyValue th = (DefaultKeyValue) t.headers;
        DefaultKeyValue mh = (DefaultKeyValue) m.headers;
        if (th.keySet().size() != mh.keySet().size()) {
            return false;
        } else {
            Set<String> keyset = th.keySet();
            for (String k : keyset) {
                if (!mh.containsKey(k) || !mh.getObject(k).equals(th.getObject(k))) {
                    return false;
                }
            }
        }

        // properties
        DefaultKeyValue tp = (DefaultKeyValue) t.properties;
        DefaultKeyValue mp = (DefaultKeyValue) m.properties;
        if (tp == null && mp == null) {
            // fine, next please,
        } else if (tp != null && mp != null && tp.keySet().size() == mp.keySet().size()) {
            Set<String> keyset = tp.keySet();
            for (String k : keyset) {
                if (!mp.containsKey(k) || !mp.getObject(k).equals(tp.getObject(k))) {
                    return false;
                }
            }
        } else {
            return false;
        }

        // body
        byte[] tb = t.getBody();
        byte[] mb = m.getBody();
        if (tb == null && mb == null) {
            return true;
        } else if (tb != null && mb != null && tb.length == mb.length) {
            for (int i = 0; i < tb.length; i++) {
                if (tb[i] != mb[i]) {
                    System.out.println("body content fail");
                    return false;
                }
            }
            return true;
        } else {
            System.out.println("body fail");
            return false;
        }
    }

}
