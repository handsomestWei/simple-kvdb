package com.wjy.simple.kvdb;

import com.wjy.simple.kvdb.entity.User;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SimpleKvDbTest {

    public static void main(String[] args) throws IOException {
        String dbName = "test";
        String dbPath = "D:\\IdeaProjects\\simple-kvdb\\";
        SimpleKvDb simpleKvDb = new SimpleKvDb(dbName, dbPath) {
            @Override
            public byte[] toBytes(Object data) throws IOException {
                if (data instanceof User) {
                    User user = (User) data;
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
                    dataOutputStream.writeUTF(user.getUserId());
                    dataOutputStream.writeUTF(user.getUserName());
                    dataOutputStream.writeInt(user.getAge());
                    return byteArrayOutputStream.toByteArray();
                } else {
                    return null;
                }

            }
        };
        User userA = new User("userA", "小明", 20);
        simpleKvDb.setData("userA", userA);
        User userB = new User("userB", "小张", 21);
        simpleKvDb.setData("userB", userB);
        System.out.println(new String(simpleKvDb.getData("userB")));
        simpleKvDb.close();
    }

}
