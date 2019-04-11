#coding=utf-8
import pymysql

def getMQL(dev_list):
    fp = open("C:\Users\max\Desktop\dd.txt", 'w')
    connect = pymysql.connect(
        host = '***********',
        port = ****,
        user = '***********',
        passwd = '*****',
        db = 'SENSOR1',
        charset = 'utf8'
    )
    cursor = connect.cursor()    #connect mysql

    sql = "SELECT DEV_ID,AVG(VER)VER " \
          "FROM DEVICE_CAPTURE_DATA_2_201904 " \
          "WHERE DEV_ID = %s  AND CAP_TIME >= '2019-04-11 18:00:00'" \
          "GROUP BY CAP_TIME ORDER BY CAP_TIME DESC; "
    r = 0
    for devid in dev_list :
        try:
            cursor.execute(sql, (devid[:-1]))
        except Exception as e:
            connect.rollback()
            print 'execute error'
        else:
            connect.commit()
            print 'execute success'
            print  devid[:-1] ,'共查找出', cursor.rowcount, '条数据'
            if cursor.rowcount == 0:
                fp.write(devid[:-1] + '     ' + 'NULL'+ '\n')
            else:
                for row_list in cursor.fetchall():
                    for d_ver in row_list:
                        if r == 1:
                            fp.write(devid[:-1] + '     ' + str(d_ver)+ '\n')
                        r = r+1
        r = 0
    cursor.close()
    connect.close()
    fp.close()

if __name__== '__main__':
    dev_list = []
    fp = open("C:\Users\max\Desktop\devid.txt", 'r')
    dev_list = fp.readlines();
    print len(dev_list)
    getMQL(dev_list)
    fp.close()



