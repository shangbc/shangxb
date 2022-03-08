# Time     2022/03/02 11::35
# Auhtor
# ide      PyCharm
# Verion   1.0
# function 默认

from ProgramConfigurationOnly import Config, OraclePolls
import datetime
import threading

# 获取线程锁
threadingLock = threading.Lock()
threadingBool = True

programCode = "DefaultData"
user = "bossmdy"
region = '570'
numerId = 0

regionCode = {
    '570': 'QUZ',
    '571': 'HZ',
    '572': 'HUZ',
    '573': 'JXI',
    '574': 'NBO',
    '575': 'SHX',
    '576': 'TZH',
    '577': 'WZH',
    '578': 'LSH',
    '579': 'JIH',
    '580': 'ZSH'
}


# 获取用户映射
def getRegionUser():
    regionUserSql = "select * from bossmdy.region_users t where t.service_name = 'crm'"
    regionUserDataList = ora.selectSqlExecute('cboss', regionUserSql)

    regionUserDict = {}
    for regionUserData in regionUserDataList:
        if regionUserData['SERVICE_NAME'] not in regionUserDict:
            regionUserDict[regionUserData['SERVICE_NAME']] = {}
        regionDict = {
            regionUserData['REGION_NAME']: regionUserData
        }
        regionUserDict[regionUserData['SERVICE_NAME']].update(regionDict)
    return regionUserDict


def getDataSourceMapping():
    try:
        global conDict
        getSourceSql = """
                       select t.* from {user}.program_sourct_config t
                       where t.config_state = 'U'
                             and t.python_code = '{programCode}'
                       """.format(user=user, programCode=programCode)

        sourceDateList = ora.selectSqlExecute('cboss', getSourceSql)

        if len(sourceDateList) == 0:
            raise Exception('program_sourct_config配置错误')

        sourceDict = {}
        for sourceDate in sourceDateList:
            sourceDict.update({
                sourceDate['CONFIG_CODE']: sourceDate['CONFIG_VAULE']
            })

        if 'DATASOURCE' not in sourceDict:
            raise Exception('program_sourct_config配置中没有datasource节点')

        conDict = sourceDict

        sourceNameList = "'" + "','".join(sourceDict['DATASOURCE'].split(';')) + "'"

        getMapSql = """
                    select t.*,rowid from {user}.Data_source_mapping t
                    where t.map_name in ({sourceNameList})
                          and t.state = 'U'
                    """.format(user=user, sourceNameList=sourceNameList)
        mapDataList = ora.selectSqlExecute('cboss', getMapSql)

        mapList = []
        for mapData in mapDataList:
            mapDict = {
                'userName': mapData['MAP_NAME'],
                'linkName': mapData['LINK_NAME'],
                'state': '0' if mapData['SOURCE_TYPE'].upper() == 'ORACLE' else '1'
            }
            mapList.append(mapDict)

        return mapList
    except Exception as error:
        logging.error(error)
        raise Exception('加载新的数据源映射失败')


def getCrmData():
    try:
        dropSql = "drop table devops_aigoc.bxr_quanwang_[:region]"
        dropDict = {
            'region': region
        }

        try:
            ora.sqlExecute(dbsourceName, dropSql, dropDict)
        except Exception as error:
            logging.error(error)

        selectSql = """
                    create table devops_aigoc.bxr_quanwang_[:region] as
                    select distinct 'null' SEQ_ID,
                                    '01' OPR,
                                    sysdate OPR_TIME,
                                    a.bill_id PHONE_NUM,
                                    case
                                      when ('5' = substr(a.bill_id, 0, 1)) then
                                       '1'
                                      else
                                       '2'
                                    end PHONE_TYPE,
                                    case
                                      when instr(d.os_status, '1') = 0 then
                                       '2'
                                      else
                                       '4'
                                    end USER_STATUS,
                                    a.done_date USER_STATUS_TIME,
                                    a.create_date EFFECTIVE_TIME,
                                    a.active_date ACTIVATION_TIME,
                                    '571' PROVINCE,
                                    '[:regionCode]' CITY,
                                    '571' KKSF,
                                    'null' KKDS,
                                    'null' QDLX,
                                    'null' RWQD,
                                    'null' QDMC,
                                    to_char(a.create_org_id) WDBH,
                                    a.create_op_id KKGH,
                                    'null' YYTHDLSSZWZ,
                                    'null' XHSJ,
                                    a.sub_bill_id IMSI,
                                    e.icc_id ICCID,
                                    'NULL' IMEI,
                                    a.user_id PROV_SUBSID,
                                    a.cust_id PROV_CUSTID,
                                    c.acct_id PROV_ACCTID,
                                    case
                                      when f.cust_cert_type not in (11, 13, 14, 15, 16, 26) then
                                       f.cust_cert_type
                                      else
                                       g.owner_cert_type
                                    end ID_CARD_TYPE,
                                    case
                                      when f.cust_cert_type not in (11, 13, 14, 15, 16, 26) then
                                       f.cust_cert_code
                                      else
                                       g.owner_cert_code
                                    end ID_CARD_NUM,
                                    case
                                      when f.cust_cert_type not in (11, 13, 14, 15, 16, 26) then
                                       f.cust_name
                                      else
                                       g.owner_person
                                    end CUSTOMER_NAME,
                                    'NULL' STATES,
                                    'NULL' BUSINESS_ID,
                                    'NULL' N_EXT1,
                                    'NULL' N_EXT2,
                                    'NULL' N_EXT3,
                                    'NULL' V_EXT1,
                                    'NULL' V_EXT2,
                                    'NULL' V_EXT3
                      from  [:userName].ins_user_[:region] a left join [:userName].ins_accrel_[:region]  c on a.user_id = c.user_id
                            left join [:userName].ins_user_os_state_[:region]  d on a.user_id = d.user_id
                            left join [:userName].res_sim_card_used_[:region]  e on a.sub_bill_id = e.imsi
                            left join [:userName].cm_Indiv_customer_[:region]  f on a.cust_id  =f.indiv_cust_id
                            left join [:userName].insx_owner_info_[:region]  g on a.user_id  =g.user_id
                     where  c.expire_date>sysdate 
                          and c.pay_type =1
                       and a.state in ('1','4')
                       and  a.expire_date>sysdate
                       and  d.expire_date >sysdate 
                       and a.prod_catalog_id = '507000000180' 
                       and  g.expire_date >sysdate
                       and  g.state =1
                       and a.create_date between trunc(sysdate -3,'dd') and trunc(sysdate-2,'dd')
                    """
        selectDict = {
            'region': region,
            'userName': userName,
            'regionCode': regionCode[region]
        }
        ora.sqlExecute(dbsourceName, selectSql, selectDict)
    except Exception as error:
        ora.dataRollback()
        logging.error(error)
    else:
        ora.dataCommit()


class MyThreading(threading.Thread):
    def __init__(self, index):
        threading.Thread.__init__(self)
        self.setName("MyThreading-" + index)

    def run(self):
        global threadingBool
        while (threadingBool):
            dataSyncBomc()


def dataSyncBomc():
    try:
        global numerId, threadingBool
        threadingLock.acquire()
        numerIdStart = numerId
        numerIdEnd = numerId + 1000
        numerId += 1000
        threadingLock.release()

        threadingDb = OraclePolls(mapList, logging)
        selectSql = """
                    select q.seq_id,
                           q.opr,
                           q.opr_time,
                           q.phone_num,
                           q.phone_type,
                           q.user_status,
                           q.user_status_time,
                           q.effective_time,
                           q.activation_time,
                           q.province,
                           q.city,
                           q.kksf,
                           q.kkds,
                           q.qdlx,
                           q.rwqd,
                           q.qdmc,
                           q.wdbh,
                           q.kkgh,
                           q.yythdlsszwz,
                           q.xhsj,
                           q.imsi,
                           q.iccid,
                           q.imei,
                           q.prov_subsid,
                           q.prov_custid,
                           q.prov_acctid,
                           q.id_card_type,
                           q.id_card_num,
                           q.customer_name,
                           q.states,
                           q.business_id,
                           q.n_ext1,
                           q.n_Ext2,
                           q.n_ext3,
                           q.v_ext1,
                           q.v_ext2,
                           q.v_ext3
                      from (select rownum as numerId, t.* from devops_aigoc.bxr_quanwang_[:region] t) q
                     where q.numerId between [:numerIdStart] and [:numerIdEnd]
                    """
        selectDict = {
            'region': region,
            'numerIdStart': str(numerIdStart),
            'numerIdEnd': str(numerIdEnd)
        }
        selectDate = threadingDb.selectSqlExecute(dbsourceName, selectSql, selectDict)

        if len(selectDate) == 0:
            threadingBool = False
            return
        logging.info(selectDate)
        insertTable = 'bomc.bxr_quanwang_' + region

        threadingDb.batchInsertAll('pd', insertTable, selectDate)
    except Exception as error:
        threadingDb.dataRollback()
        logging.error(error)
    else:
        threadingDb.dataCommit()
    finally:
        threadingDb.dataClose()


def getResultData():
    try:
        dropSql = "drop table bomc.bxr_result_{region}_2022".format(region=region)
        try:
            ora.sqlExecute('pd', dropSql)
        except Exception as error:
            logging.error(error)

        sqlectSql = """
                    create  table  bomc.bxr_result_{region}_2022 parallel 6  as
                    select /*+ parallel(6)*/
                    distinct 1000000000 + rownum SEQ_ID,
                             a.opr,
                             a.opr_time,
                             a.phone_num,
                             a.phone_type,
                             a.user_status,
                             a.user_status_time,
                             a.effective_time,
                             a.activation_time,
                             a.province,
                             a.city,
                             a.kksf,
                             b.jtorz KKDS,
                             b.channel_type QDLX,
                             b.network_type RWQD,
                             case
                               when b.organize_name is null then
                                '0000'
                               else
                                b.organize_name
                             end QDMC,
                             a.wdbh,
                             a.kkgh,
                             b.org_address yythdlsszwz ,
                             a.xhsj,
                             a.imsi,
                             a.iccid,
                             a.imei,
                             a.prov_subsid,
                             a.prov_custid,
                             a.prov_acctid,
                             c.code_name id_card_type,
                             a.id_card_num,
                             a.customer_name,
                             a.states,
                             a.business_id,
                             a.n_ext1,
                             a.n_ext2,
                             a.n_ext3,
                             a.v_ext1,
                             a.v_ext2,
                             a.v_ext3
                      from bomc.bxr_quanwang_{region} a,
                           baomjsec_organize_ext b,
                           baomjBS_STATIC_DATA   c

                     where a.wdbh = b.organize_id(+)
                       and a.id_card_type = c.code_value(+)
                       and c.code_type(+) = 'LOCAL_CERTTYPE_MAPPING'
                    """ \
            .format(region=region)
        ora.sqlExecute('pd', sqlectSql)

        deleteSql = """
                    delete bomc.bxr_result_{region}_2022 t
                    where t.activation_time is null
                          or t.effective_time is null 
                          or t.user_status_time is null 
                          or t.activation_time is null
                          or t.id_card_num is null 
                          or t.customer_name is null
                    """.format(region=region)
        ora.sqlExecute('pd', deleteSql)
    except Exception as error:
        ora.dataRollback()
        logging.error(error)
    else:
        ora.dataCommit()


def insterData():
    try:
        nowData = datetime .datetime .now()
        nowDataStr = nowData .strftime('%Y%m')
        selecttSql = """
                      select SEQ_ID,
                             OPR,
                             OPR_TIME,
                             PHONE_NUM,
                             PHONE_TYPE,
                             USER_STATUS,
                             USER_STATUS_TIME,
                             EFFECTIVE_TIME,
                             ACTIVATION_TIME,
                             PROVINCE,
                             CITY,
                             KKSF,
                             KKDS,
                             QDLX,
                             RWQD,
                             QDMC,
                             WDBH,
                             KKGH,
                             YYTHDLSSZWZ,
                             IMSI,
                             ICCID,
                             IMEI,
                             PROV_SUBSID,
                             PROV_CUSTID,
                             PROV_ACCTID,
                             ID_CARD_TYPE,
                             ID_CARD_NUM,
                             CUSTOMER_NAME
                        from bomc.bxr_result_{region}_2022
                    """ .format(region=region)
        selecttData = ora .selectSqlExecute('pd', selecttSql)

        if len(selecttData) == 0:
            return
        insertTable = 'PARTY.QW_CHANL_INFO_SYNC_INCR_{month}' .format(month=nowDataStr)
        ora .batchInsertAll('party', insertTable, selecttData)
    except Exception as error:
        ora .dataClose()
        logging .error(error)
    else:
        ora .dataCommit()

def exce():
    getCrmData()
    truncate = 'truncate table bomc.bxr_quanwang_' + region
    try:
        ora.sqlExecute('pd', truncate)
    except Exception as error:
        ora.dataRollback()
        logging.error(error)
    else:
        ora.dataCommit()

    threadingMax = 10
    threadingList = []
    for index in range(threadingMax):
        myThreading = MyThreading(index.__str__())
        threadingList.append(myThreading)
        logging.info('MyThreading-' + str(index) + '创建成功')

    for myThreading in threadingList:
        myThreading.start()

    for myThreading in threadingList:
        myThreading.join()
    getResultData()

    insterData()

    logging.info('程序执行结束')


if __name__ == '__main__':
    try:
        # 读取配置EquityRepairAutidNew
        config = Config(programCode)
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()

        # 加载新的数据源配置映射
        mapList = getDataSourceMapping()

        # 获取地市数据列表
        regionUser = getRegionUser()

        # 获取当前地市的用户名和数据源名称
        userName = regionUser['crm'][int(region)]['USER_NAME']
        dbsourceName = regionUser['crm'][int(region)]['DBSOURCE_NAME']

        exce()

    except Exception as error:
        logging.error(error)
    finally:
        if ora is not None:
            ora.dataClose()