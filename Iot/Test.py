# import jaydebeapi
from DBUtils.PooledDB import PooledDB
import pymysql

# dirver = "com.alipay.oceanbase.jdbc.Driver"
# url = "jdbc:oceanbase://10.70.168.110:3306/BOSSMDY?rewriteBatchedStateme&useUnicode=true&characterEncoding=utf-8"
# user = "BOSSMDY@YWDTA#ywdt_obcluster"
# password = "!QAZ2wsx"
# jarFile = "C://Users//Lenovo//Desktop//down//oceanbase-client-1.1.10.jar"
# conn = jaydebeapi.connect(dirver,url,[user,password],jarFile)
#
#
# curs = conn.cursor()
# curs.execute("select to_char(sysdate,'yyyy-MM-dd HH24:mi:ss') from dual;")
# rs = curs.fetchall()
# for r in rs:
#     print(r)
# curs.close()
# conn.close()
oraclePool = PooledDB(creator=pymysql, maxconnections=None, mincached=2, maxcached=10,
                                          maxshared=3,
                                          blocking=True, maxusage=None, setsession=[], ping=0, host='20.26.208.3',
                                          port=int(2883),
                                          user='uorder3@ordera#ywttest', password='order3_1Q#', database='ywttest', charset='UTF8')