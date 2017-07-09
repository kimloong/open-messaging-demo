package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.exception.OMSRuntimeException;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static io.openmessaging.MessageHeader.*;

/**
 * Created by KimLoong on 17-6-28.
 */
public class OMSCustomSerializer implements OMSSerializer {

    public static final Charset CHARSET = Charset.forName("UTF-8");

    private static final int BYTE_BUFFER_POOL_SIZE = 20;
    //用于在序列化时保存header与properties的ByteBuffer池
    private final BlockingQueue<ByteBuffer> byteBufferPool = new LinkedBlockingDeque<>(BYTE_BUFFER_POOL_SIZE);

    public OMSCustomSerializer(int maxMessageSize) {
        for (int i = 0; i < BYTE_BUFFER_POOL_SIZE; i++) {
            byteBufferPool.add(ByteBuffer.allocate(maxMessageSize / 2));
        }
    }

    @Override
    public void serializeMessage(Message message, ByteBuffer byteBuffer) {
        if (message instanceof BytesMessage) {
            ByteBuffer otherBuffer = null;
            try {
                BytesMessage bytesMessage = (BytesMessage) message;


                otherBuffer = byteBufferPool.take();
                otherBuffer.clear();
                int headerLength = serializeHeaders(bytesMessage.headers(), otherBuffer);
                int propertiesLength = serializeProperties(bytesMessage.properties(), otherBuffer);

                int totalLength = bytesMessage.getBody().length + headerLength
                        + propertiesLength + 8;
                byteBuffer.putInt(totalLength);
                byteBuffer.putInt(headerLength);
                byteBuffer.putInt(propertiesLength);
                byteBuffer.put(bytesMessage.getBody());
                otherBuffer.flip();
                byteBuffer.put(otherBuffer);

            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new OMSRuntimeException();
            } finally {
                if (null != otherBuffer) {
                    byteBufferPool.add(otherBuffer);
                }
            }

        } else {
            throw new UnsupportedOperationException("unsupported serialize not BytesMessage");
        }
    }

    @Override
    public Message deserializeMessage(ByteBuffer byteBuffer, int totalLength) {
        int headerLength = byteBuffer.getInt();
        int propertiesLength = byteBuffer.getInt();
        int bodyLength = totalLength - headerLength - propertiesLength - 8;

        byte[] body = new byte[bodyLength];
        byteBuffer.get(body);

        BytesMessage bytesMessage = new DefaultBytesMessage(body);
        deserializeHeaders(bytesMessage, byteBuffer, headerLength);
        deserializeProperties(bytesMessage, byteBuffer, propertiesLength);
        return bytesMessage;
    }

    private static int serializeHeaders(KeyValue headers, ByteBuffer byteBuffer) {
        if (null == headers) {
            return 0;
        }
        int startPosition = byteBuffer.position();
        for (String key : headers.keySet()) {
            switch (key) {
                //针对不同的键，采用不同的序列化策略
                case TOPIC:
                    serializeStringHeader((byte) 0, headers.getString(key), byteBuffer);
                    break;
                case QUEUE:
                    serializeStringHeader((byte) 1, headers.getString(key), byteBuffer);
                    break;
                case MESSAGE_ID:
                    serializeStringHeader((byte) 2, headers.getString(key), byteBuffer);
                    break;
                case BORN_TIMESTAMP:
                    serializeLongHeader((byte) 3, headers.getLong(key), byteBuffer);
                    break;
                case BORN_HOST:
                    serializeStringHeader((byte) 4, headers.getString(key), byteBuffer);
                    break;
                case STORE_TIMESTAMP:
                    serializeLongHeader((byte) 5, headers.getLong(key), byteBuffer);
                    break;
                case STORE_HOST:
                    serializeStringHeader((byte) 6, headers.getString(key), byteBuffer);
                    break;
                case START_TIME:
                    serializeLongHeader((byte) 7, headers.getLong(key), byteBuffer);
                    break;
                case STOP_TIME:
                    serializeLongHeader((byte) 8, headers.getLong(key), byteBuffer);
                    break;
                case TIMEOUT:
                    serializeIntHeader((byte) 9, headers.getInt(key), byteBuffer);
                    break;
                case PRIORITY:
                    serializeByteHeader((byte) 10, (byte) headers.getInt(key), byteBuffer);
                    break;
                case RELIABILITY:
                    serializeIntHeader((byte) 11, headers.getInt(key), byteBuffer);
                    break;
                case SEARCH_KEY:
                    serializeStringHeader((byte) 12, headers.getString(key), byteBuffer);
                    break;
                case SCHEDULE_EXPRESSION:
                    serializeStringHeader((byte) 13, headers.getString(key), byteBuffer);
                    break;
                case SHARDING_KEY:
                    serializeStringHeader((byte) 14, headers.getString(key), byteBuffer);
                    break;
                case SHARDING_PARTITION:
                    serializeStringHeader((byte) 15, headers.getString(key), byteBuffer);
                    break;
                case TRACE_ID:
                    serializeStringHeader((byte) 16, headers.getString(key), byteBuffer);
                    break;
                default:
                    throw new UnsupportedOperationException("unsupported header key " + key);
            }
        }
        return byteBuffer.position() - startPosition;
    }

    private void deserializeHeaders(BytesMessage bytesMessage, ByteBuffer byteBuffer, int headerLength) {
        int startPosition = byteBuffer.position();
        int limit = startPosition + headerLength;

        while (byteBuffer.position() < limit) {
            byte keyByte = byteBuffer.get();
            switch (keyByte) {
                //针对不同的键，采用不同的序列化策略
                case 0:
                    bytesMessage.putHeaders(TOPIC, deserializeStringHeader(byteBuffer));
                    break;
                case 1:
                    bytesMessage.putHeaders(QUEUE, deserializeStringHeader(byteBuffer));
                    break;
                case 2:
                    bytesMessage.putHeaders(MESSAGE_ID, deserializeStringHeader(byteBuffer));
                    break;
                case 3:
                    bytesMessage.putHeaders(BORN_TIMESTAMP, byteBuffer.getLong());
                    break;
                case 4:
                    bytesMessage.putHeaders(BORN_HOST, deserializeStringHeader(byteBuffer));
                    break;
                case 5:
                    bytesMessage.putHeaders(STORE_TIMESTAMP, byteBuffer.getLong());
                    break;
                case 6:
                    bytesMessage.putHeaders(STORE_HOST, deserializeStringHeader(byteBuffer));
                    break;
                case 7:
                    bytesMessage.putHeaders(START_TIME, byteBuffer.getLong());
                    break;
                case 8:
                    bytesMessage.putHeaders(STOP_TIME, byteBuffer.getLong());
                    break;
                case 9:
                    bytesMessage.putHeaders(TIMEOUT, byteBuffer.getInt());
                    break;
                case 10:
                    bytesMessage.putHeaders(PRIORITY, (int) byteBuffer.get());
                    break;
                case 11:
                    bytesMessage.putHeaders(RELIABILITY, byteBuffer.getInt());
                    break;
                case 12:
                    bytesMessage.putHeaders(SEARCH_KEY, deserializeStringHeader(byteBuffer));
                    break;
                case 13:
                    bytesMessage.putHeaders(SCHEDULE_EXPRESSION, deserializeStringHeader(byteBuffer));
                    break;
                case 14:
                    bytesMessage.putHeaders(SHARDING_KEY, deserializeStringHeader(byteBuffer));
                    break;
                case 15:
                    bytesMessage.putHeaders(SHARDING_PARTITION, deserializeStringHeader(byteBuffer));
                    break;
                case 16:
                    bytesMessage.putHeaders(TRACE_ID, deserializeStringHeader(byteBuffer));
                    break;
                default:
                    break;
            }
        }
    }

    private int serializeProperties(KeyValue properties, ByteBuffer byteBuffer) {
        if (null == properties) {
            return 0;
        }
        if (properties instanceof DefaultKeyValue) {
            DefaultKeyValue defaultKeyValue = (DefaultKeyValue) properties;
            int startPosition = byteBuffer.position();

            for (String key : defaultKeyValue.keySet()) {
                serializeProperty(key, defaultKeyValue.getObject(key), byteBuffer);
            }

            return byteBuffer.position() - startPosition;
        }
        throw new UnsupportedOperationException("unsupported serialize not BytesMessage");
    }


    private void deserializeProperties(BytesMessage bytesMessage, ByteBuffer byteBuffer, int propertiesLength) {
        int startPosition = byteBuffer.position();
        int limit = startPosition + propertiesLength;

        while (byteBuffer.position() < limit) {
            deserializeProperty(bytesMessage, byteBuffer);
        }
    }

    private void serializeProperty(String key, Object value, ByteBuffer byteBuffer) {
        byte[] keyBytes = key.getBytes(CHARSET);
        byteBuffer.putShort((short) keyBytes.length);
        byteBuffer.put(keyBytes);
        if (value instanceof String) {
            byte[] valueBytes = ((String) value).getBytes(CHARSET);
            int valueBytesLength = valueBytes.length;
            if (valueBytesLength > Byte.MAX_VALUE) {
                byteBuffer.put((byte) 0);
                byteBuffer.putShort((short) valueBytes.length);
            } else {
                byteBuffer.put((byte) 1);
                byteBuffer.put((byte) valueBytesLength);
            }
            byteBuffer.put(valueBytes);
        } else if (value instanceof Integer) {
            byteBuffer.put((byte) 2);
            byteBuffer.putInt((Integer) value);
        } else if (value instanceof Long) {
            byteBuffer.put((byte) 3);
            byteBuffer.putLong((Long) value);
        } else if (value instanceof Double) {
            byteBuffer.put((byte) 4);
            byteBuffer.putDouble((Double) value);
        } else {
            throw new UnsupportedOperationException("unsupported properties type "
                    + value.getClass().getName() + " of key " + key);
        }
    }

    private void deserializeProperty(BytesMessage bytesMessage, ByteBuffer byteBuffer) {
        short keyLength = byteBuffer.getShort();
        byte[] keyBytes = new byte[keyLength];
        byteBuffer.get(keyBytes);
        String key = new String(keyBytes, CHARSET);
        byte valueTypeByte = byteBuffer.get();
        switch (valueTypeByte) {
            case 0:
                short longStringBytesLength = byteBuffer.getShort();
                byte[] longStringValueBytes = new byte[longStringBytesLength];
                byteBuffer.get(longStringValueBytes);
                String longStringValue = new String(longStringValueBytes, CHARSET);
                bytesMessage.putProperties(key, longStringValue);
                break;
            case 1:
                byte valueBytesLength = byteBuffer.get();
                byte[] valueBytes = new byte[valueBytesLength];
                byteBuffer.get(valueBytes);
                String value = new String(valueBytes, CHARSET);
                bytesMessage.putProperties(key, value);
                break;
            case 2:
                bytesMessage.putProperties(key, byteBuffer.getInt());
                break;
            case 3:
                bytesMessage.putProperties(key, byteBuffer.getLong());
                break;
            case 4:
                bytesMessage.putProperties(key, byteBuffer.getDouble());
                break;
            default:
                throw new UnsupportedOperationException("unsupported properties type byte "
                        + valueTypeByte + " of key " + key);
        }
    }

    private static void serializeStringHeader(byte headerByte, String value, ByteBuffer byteBuffer) {
        byteBuffer.put(headerByte);
        byte[] valueBytes = value.getBytes(CHARSET);
        byteBuffer.putShort((short) valueBytes.length);
        byteBuffer.put(valueBytes);
    }

    private String deserializeStringHeader(ByteBuffer byteBuffer) {
        short length = byteBuffer.getShort();
        byte[] bytes = new byte[length];
        byteBuffer.get(bytes);
        return new String(bytes, CHARSET);
    }

    private static void serializeLongHeader(byte headerByte, long value, ByteBuffer byteBuffer) {
        byteBuffer.put(headerByte);
        byteBuffer.putLong(value);
    }

    private static void serializeIntHeader(byte headerByte, int value, ByteBuffer byteBuffer) {
        byteBuffer.put(headerByte);
        byteBuffer.putInt(value);
    }

    private static void serializeByteHeader(byte headerByte, byte value, ByteBuffer byteBuffer) {
        byteBuffer.put(headerByte);
        byteBuffer.put(value);
    }
}
