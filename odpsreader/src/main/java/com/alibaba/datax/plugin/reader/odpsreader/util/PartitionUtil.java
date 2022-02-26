package com.alibaba.datax.plugin.reader.odpsreader.util;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.aliyun.odps.account.AliyunAccount;
import java.util.Date;
import java.util.List;

/**
 * @author nkong
 * @version V1.0
 * @date 2021/6/17
 */
public class PartitionUtil {

    /**
     * 分区模式处理，例如：yyyymmdd 表示当天日期
     */
    public static String formatPartition(String part, Odps odps, String project, String table) {

        if ("maxPt".equalsIgnoreCase(part)) {
            return getMaxPartition(odps, project, table);
        }

        if (part.toLowerCase().contains("yyyymmdd")) {
            return getFirstPartition(part);
        }

        return part;
    }

    /**
     * 格式化第一级分区
     * 例如：yyyymmdd - 1 格式化为当天日期减1天
     *
     * @param part 自定义分区格式
     * @return 格式化后的分区值
     */
    private static String getFirstPartition(String part) {
        String[] first = part.split("=");
        String[] split = first[1].split("-");
        String date;
        if (split.length == 2) {
            date = DateTool.subDay(DateTool.formatDate(new Date()), split[1].trim());
        } else {
            date = DateTool.formatDate(new Date());
        }
        part = part.replace(first[1], date);
        return part;
    }

    /**
     * 获取表的最大分区
     *
     * @param odps odps
     * @param project 项目空间
     * @param table 表名
     * @return 最大分区
     */
    private static String getMaxPartition(Odps odps, String project, String table) {
        List<Partition> partitions = odps.tables().get(project, table).getPartitions();
        Partition partition = partitions.get(partitions.size() - 1);
        return partition.getPartitionSpec().toString().split(",", -1)[0];
    }

    public static void main(String[] args) {
        AliyunAccount account =
            new AliyunAccount("3dVcAZIqyKqnwaNV", "DzJOutCxvk2gWC2s9jKr1lZksTeO7J");
        Odps odps = new com.aliyun.odps.Odps(account);
        odps.setEndpoint("http://139.36.0.244/api");
        System.out.println(formatPartition("ds=20210851", odps, "ysk_wl", "fh_ods_face_base"));
    }
}
