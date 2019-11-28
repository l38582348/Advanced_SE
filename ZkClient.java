package com.roger.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZkClient {

    private ZooKeeper zkCli;
    private static final String CONNECT_STRING = "hadoop131:2181,hadoop132:2181,hadoop133:2181";
    private static final int SESSION_TIMEOUT = 2000;

    @Before
    public void before() throws IOException {
        zkCli = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, e -> {
            System.out.println("默认回调函数");
    });
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        List<String> children = zkCli.getChildren("/", e ->  {
            System.out.println("自定义回调函数");
        });

        System.out.println("==========================================");
        for (String child : children) {
            System.out.println(child);
        }
        System.out.println("==========================================");

        Thread.sleep(Long.MAX_VALUE);//卡住，等待
    }

    @Test
    public void create() throws KeeperException, InterruptedException {//创建节点 临时的
        String s = zkCli.create("/Idea", "Idea2018".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        System.out.println(s);

        Thread.sleep(Long.MAX_VALUE);

    }

    @Test
    public void get() throws KeeperException, InterruptedException {
        byte[] data = zkCli.getData("/sanguo", true, new Stat());//获取 sanguo节点

        String string = new String(data);

        System.out.println(string);

    }

    @Test
    public void set() throws KeeperException, InterruptedException {

        Stat stat = zkCli.setData("/sanguo", "zhaoyun".getBytes(), 0);//0 是 版本号，确定要改的是你看的版本仿制别人改了  你又改了


        System.out.println(stat.getDataLength());

    }

    @Test
    public void stat() throws KeeperException, InterruptedException {//节电状态 是否存在
        Stat exists = zkCli.exists("/sanguo", false);
        if (exists == null) {
            System.out.println("节点不存在");
        } else {
            System.out.println(exists.getDataLength());
        }
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        Stat exists = zkCli.exists("/zxx0000000007", false);
        if (exists != null)
            zkCli.delete("/zxx0000000007", exists.getVersion());
    }
    //循环注册小功能 显示 sanguo信息，若信息修改 也打印修改后的信息
    //监听节点变化
    public void register() throws KeeperException, InterruptedException {
        byte[] data = zkCli.getData("/sanguo", new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    register();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }, null);

        System.out.println(new String(data));
    }

    @Test
    public void testRegister() {
        try {
            register();
            Thread.sleep(Long.MAX_VALUE);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
