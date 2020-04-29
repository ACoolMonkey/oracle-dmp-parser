package com.hys.odp.util;

import com.hys.odp.model.AnalyzeTypeEnum;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Oracle dump文件解析工具类
 *
 * @author Robert Hou
 * @date 2019年3月21日
 */
@Slf4j
public class DmpAnalyzeUtils {

    private DmpAnalyzeUtils() {
    }

    /**
     * 按照readline读取dmp文件 并写入txt文件
     *
     * @param sourceFile 源文件绝对路径
     * @param aimFile    目标文件绝对路径
     */
    public static void dmpReadLine(String sourceFile, String aimFile) {
        @Cleanup RandomAccessFile raf = null;
        @Cleanup BufferedWriter out = null;
        try {
            raf = new RandomAccessFile(new File(sourceFile), "r");
            // 相对路径，如果没有则要建立一个新的output.txt文件
            File writename = new File(aimFile);
            // 创建新文件
            writename.createNewFile();
            out = new BufferedWriter(new FileWriter(writename));
            int i = 0;
            do {
                i++;
                out.write(raf.readLine() + "\r\n");
            } while (i != 20000);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static long bytesToLong(byte[] value) {
        long ret = 0;
        for (int i = 0, length = value.length; i < length; i++) {
            ret += (long) (value[value.length - i - 1] & 0xFF) << (long) (i * 8);
        }
        return ret;
    }

    public static double bytes2Double(byte[] arr) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * i);
        }
        return Double.longBitsToDouble(value);
    }

    public static int getInt(byte[] arr) {
        int first = arr[0];
        int second = arr[1];
        int secontTemp = 0;
        int firstTemp = 0;
        if (first < 0) {
            firstTemp = first + 256;
        } else if (first > 0) {
            firstTemp = first;
        }
        if (second < 0) {
            secontTemp = (second + 256) * 256;
        } else if (second > 0) {
            secontTemp = second * 256;
        }
        if (first == 0) {
            secontTemp = second;
        }
        return secontTemp + firstTemp;
    }

    /**
     * 解析varchar,varchar2,char
     */
    public static String parseString(byte[] stringByte) throws UnsupportedEncodingException {
        return new String(stringByte, "gbk");
    }

    /**
     * 解析NUMBER
     */
    public static Integer parseNumber(byte[] numberStampByte) {
        return DmpAnalyzeUtils.bytesToInt(numberStampByte);
    }

    public static int bytesToInt(byte[] value) {
        int ret = 0;
        for (int i = 0, length = value.length; i < length; i++) {
            ret += (value[value.length - i - 1] & 0xFF) << (i << 3);
        }
        return ret;
    }

    public static Timestamp parseTimeStamp(byte[] timeStampByte) {
        int tempYear = byteToInt(timeStampByte[1]);
        int year = tempYear + 1900;
        int month = byteToInt(timeStampByte[2]);
        int day = byteToInt(timeStampByte[3]);
        int tempHour = byteToInt(timeStampByte[4]);
        int hour = tempHour - 1;
        int tempMinutes = byteToInt(timeStampByte[5]);
        int minutes = tempMinutes - 1;
        int tempSecond = byteToInt(timeStampByte[6]);
        int second = tempSecond - 1;
        String tempTimestamp = year + "-" + month + "-" + day + " " + hour + ":" + minutes + ":" + second;
        return Timestamp.valueOf(tempTimestamp);
    }

    /**
     * 解析blob
     */
    public static int parseBlob(byte[] blobByte) {
        return -3;
    }

    /**
     * 解析clob
     */
    public static int parseClob(byte[] clobByte) {
        return -3;
    }

    /**
     * 字节转换int
     *
     * @return int
     */
    public static int byteToInt(byte b) {
        return b & 0xFF;
    }

    /**
     * 读取到指定的字节位置
     *
     * @param raf RandomAccessFile
     */
    public static byte[] readAndJudge(RandomAccessFile raf, byte[] target) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[target.length];
        //为什么 buf 不能代替 temp  因为 buf的长度 是根据target决定的 
        //temp起到标志的作用
        byte[] temp = new byte[5];
        do {
            System.arraycopy(buf, 1, buf, 0, buf.length - 1);
            System.arraycopy(temp, 1, temp, 0, temp.length - 1);
            byte cur = raf.readByte();
            //当我切换为真实数据模式的时候 已经开始读取第一条的数据行了
            out.write(cur);
            buf[buf.length - 1] = cur;
            temp[temp.length - 1] = cur;
            //为什么不放在后面 因为如果0 0 0 0 0 满足后 直接跳出while啦 就不能切换模式了
            if (Arrays.equals(DmpAnalyzeHelper.CREATESTATE_REALDATA, temp)) {
                //切换到数据模式
                //为什么切换为数据模式的时候 不直接
                DmpAnalyzeHelper.setPatternFlag(AnalyzeTypeEnum.REALDATA);
                return temp;
            } else if (Arrays.equals(DmpAnalyzeHelper.REALDATA_UNDATA, temp) || Arrays.equals(DmpAnalyzeHelper.REALDATA_UNDATA2, temp)) {
                //切换到无用模式
                long filePointer = raf.getFilePointer();
                DmpAnalyzeHelper.setPatternFlag(AnalyzeTypeEnum.UNDATA);
                return temp;
            }
        } while (out.size() < target.length || !Arrays.equals(buf, target));
        return out.toByteArray();
    }

    /**
     * 切换模式 未被使用
     */
    public static void transPattern(byte[] target) {
        if (Arrays.equals(DmpAnalyzeHelper.CREATESTATE_REALDATA, target)) {
            log.debug("切换到数据模型");
            DmpAnalyzeHelper.setPatternFlag(AnalyzeTypeEnum.REALDATA);
        } else if (Arrays.equals(DmpAnalyzeHelper.REALDATA_UNDATA, target)) {
            log.debug("切换到无用模式");
            //切换无用模式的同时 应该立即停止数据模式的继续执行
            DmpAnalyzeHelper.setPatternFlag(AnalyzeTypeEnum.UNDATA);
        }
    }

    /**
     * 序列化反序列化克隆
     *
     * @param obj 需要克隆的对象
     * @return 克隆后的对象
     */
    @SuppressWarnings("unchecked")
    public static <T> T clone(T obj) {
        if (obj == null) {
            throw new RuntimeException("obj为空");
        }
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bout);
            oos.writeObject(obj);
            ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bin);
            return (T) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
