/**
 * Created by Aïmène on 03/11/2016.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Options;

public class HBaseTest
{

    static final String DISPLAY = "===========================================";

    private static Configuration conf = null;

    /**
     * Initialization
     */
    static
    {
        conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
    }

    /**
     * Create a table
     */
    public static void createTable(String tableName, String[] familys)
            throws Exception
    {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName))
        {
            System.out.println("table already exists!");
        } else
        {
            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            for (int i = 0; i < familys.length; i++)
            {
                tableDesc.addFamily(new HColumnDescriptor(familys[i]));
            }
            admin.createTable(tableDesc);
            System.out.println("create table " + tableName + " ok.");
        }
    }

    /**
     * Delete a table
     */
    public static void deleteTable(String tableName) throws Exception
    {
        try
        {
            HBaseAdmin admin = new HBaseAdmin(conf);
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("delete table " + tableName + " ok.");
        } catch (MasterNotRunningException e)
        {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Put (or insert) a row
     */
    public static void addRecord(String tableName, String rowKey,
                                 String family, String qualifier, String value) throws Exception
    {
        try
        {
            HTable table = new HTable(conf, tableName);
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            System.out.println("insert recored " + rowKey + " to table "
                    + tableName + " ok.");
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    /**
     * Delete a row
     */
    public static void delRecord(String tableName, String rowKey)
            throws IOException
    {
        HTable table = new HTable(conf, tableName);
        List<Delete> list = new ArrayList<Delete>();
        Delete del = new Delete(rowKey.getBytes());
        list.add(del);
        table.delete(list);
        System.out.println("del recored " + rowKey + " ok.");
    }

    /**
     * Get a row
     */
    public static void getOneRecord(String tableName, String rowKey) throws IOException
    {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(rowKey.getBytes());
        Result rs = table.get(get);

        System.out.println(rowKey);
        for (KeyValue kv : rs.raw())
        {
            //System.out.print(new String(kv.getRow()) + " ");
            System.out.print(new String(kv.getFamily()) + ":");
            System.out.print(new String(kv.getQualifier()) + " ");
            //System.out.print(kv.getTimestamp() + " " );
            System.out.println(new String(kv.getValue()));
        }
    }

    /**
     * Scan (or list) a table
     */
    public static void getAllRecord(String tableName)
    {
        try
        {
            HTable table = new HTable(conf, tableName);
            Scan s = new Scan();
            ResultScanner ss = table.getScanner(s);
            for (Result r : ss)
            {
                Boolean started = false; // see next comment
                for (KeyValue kv : r.raw())
                {
                    // Display first name only once, then only the attributes
                    if(!started)
                    {
                        System.out.println(new String(kv.getRow()));
                        started = true;
                    }
                    System.out.print(new String(kv.getFamily()) + ":");
                    System.out.print(new String(kv.getQualifier()) + " ");
                    //System.out.print(kv.getTimestamp() + " ");
                    System.out.println(new String(kv.getValue()));
                }
            }
        } catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    public static void AddUser(String tableName) throws Exception
    {
        // Attributes in Info family
        String firstName, birthDate, phoneNumber, address;
        String[] hobbies;

        // Best friend and other friends
        String bff;
        String[] otherFriends;

        // Use a scanner to get instructions from the user
        Scanner userInput = new Scanner(System.in);

        // First name
        do
        {
            System.out.print("New User's First Name (Mandatory): ");
            firstName = userInput.nextLine();
        } while (firstName.equals(""));

        // Info
        System.out.print(firstName + "'s Birthdate (MMDDYYYY): ");
        birthDate = userInput.nextLine();

        System.out.print(firstName + "'s Phone Number (only numbers): ");
        phoneNumber = userInput.nextLine();

        System.out.print(firstName + "'s Address: ");
        address = userInput.nextLine();

        // Friends
        do
        {
            System.out.print(firstName + "'s Best Friend (Mandatory): ");
            bff = userInput.nextLine();
        } while (bff.equals(""));

        System.out.print(firstName + "'s Other Friend (all first names separated by ','): ");
        otherFriends = userInput.nextLine().split(",");

        HBaseTest.addRecord(tableName, firstName, "info", "birthDate", birthDate);
        HBaseTest.addRecord(tableName, firstName, "info", "phoneNumber", phoneNumber);
        HBaseTest.addRecord(tableName, firstName, "info", "address", address);
        HBaseTest.addRecord(tableName, firstName, "friends", "bff", bff);

        /* Need to convert the string array to a Byte
        for (String s:otherFriends)
        {
            HBaseTest.addRecord(tableName, firstName, "friends", "otherFriends", s);
        }*/
    }

    public static void main(String[] agrs)
    {
        try
        {
            Scanner userInput = new Scanner(System.in);
            char instruction;

            // Table user
            String tableName = "user";

            //Infos and Friends are groups of attributes
            String[] families = {"info", "friends"};
            HBaseTest.createTable(tableName, families);

            Boolean exit = false;

            System.out.println("\n" + DISPLAY + "\nWelcome to HNetwork, the best social network!\n");

            do
            {
                System.out.println(DISPLAY + "\n1: Add a new user\n2: View a record\n3: View all records\nq: Exit\n\nType in your command (one character): ");
                instruction = userInput.nextLine().charAt(0);

                switch (instruction)
                {
                    case '1':
                        AddUser(tableName);
                        break;
                    case '2':
                        System.out.print("First Name of the user: ");
                        userInput.reset(); // clear content of scanner
                        String userName = userInput.nextLine();
                        getOneRecord(tableName, userName);
                        break;
                    case '3':
                        getAllRecord(tableName);
                        break;
                    case 'q':
                        exit = true;
                        break;
                    default:
                        System.out.println("Unknown instruction");
                        break;
                }
            } while (!exit);
        } catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}