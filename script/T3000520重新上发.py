# Time     2022/01/19 15::40
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ProgramConfigurationOnly import Config
from ProgramConfigurationOnly import OraclePolls

programCode = "CtrmPromptnessMonitor"
user = "bossmdy"


def getDataSourceMapping():
    try:
        global conDict
        getSourceSql = """
                       select t.* from {user}.program_sourct_config t
                       where t.config_state = 'U'
                             and t.python_code = '{programCode}'
                       """ .format(user=user, programCode=programCode)

        sourceDateList = ora .selectSqlExecute('cboss', getSourceSql)

        if len(sourceDateList) == 0:
            raise Exception('program_sourct_config配置错误')

        sourceDict = {}
        for sourceDate in sourceDateList:
            sourceDict .update({
                sourceDate['CONFIG_CODE']: sourceDate['CONFIG_VAULE']
            })

        if 'DATASOURCE' not in sourceDict:
            raise Exception('program_sourct_config配置中没有datasource节点')

        conDict = sourceDict

        sourceNameList = "'" + "','" .join(sourceDict['DATASOURCE'].split(';')) + "'"

        getMapSql = """
                    select t.*,rowid from {user}.Data_source_mapping t
                    where t.map_name in ({sourceNameList})
                          and t.state = 'U'
                    """ .format(user=user, sourceNameList = sourceNameList)
        mapDataList = ora .selectSqlExecute('cboss', getMapSql)

        mapList = []
        for mapData in mapDataList:
            mapDict = {
                'userName': mapData['MAP_NAME'],
                'linkName': mapData['LINK_NAME'],
                'state': '0' if mapData['SOURCE_TYPE'] .upper() == 'ORACLE' else '1'
            }
            mapList .append(mapDict)

        return mapList
    except Exception as error:
        logging .error(error)
        raise Exception('加载新的数据源映射失败')


def getSoNbr(billId, goodsId):
    selectSql = """
                select * from cboss.i_user_radius_index_his a
                where  a.process_date < to_date('2022-01-15 00:00:00','yyyy-mm-dd hh24:mi:ss')
                       and a.process_date >= to_date('2022-01-14 00:00:00','yyyy-mm-dd hh24:mi:ss') 
                       and a.busi_type = '83'
                       and a.bill_id = '{billId}'
                order by a.process_date desc
                """ .format(billId=billId)
    selectData = ora .selectSqlExecute('cboss', selectSql)

    soNbl = None

    for data in selectData:
        nbl = data['SO_NBR']
        sql = """
              select * from cboss.i_user_radius_his t
                where t.commit_date < to_date('2022-01-15 00:00:00','yyyy-mm-dd hh24:mi:ss')
                       and t.commit_date >= to_date('2022-01-14 00:00:00','yyyy-mm-dd hh24:mi:ss')
                       and t.property_id = '521004'
                       and t.so_nbr = '{nbl}'
              """ .format(nbl=nbl)
        radius = ora .selectSqlExecute('cboss', sql)
        goods = radius[0]['property_value']
        logging .info(radius)
        if goods == goodsId:
            soNbl = nbl
            break

    return soNbl

def getData():
    try:
        selectSql = """
                    select t.* from bossmdy_jk1.shangxb_repair_20220119 t
                    where t.status = '0'
                    """
        selectData = ora .selectSqlExecute('cboss', selectSql)
        return selectData
    except Exception as error:
        raise error


def exec():
    try:
        repairData = getData()

        if len(repairData) == 0:
            return

        for repairi in repairData:
            try:
                mobile = repairi['MOBILE']
                goodsId = repairi['GOODS_ID']

                soNbl = getSoNbr(mobile, goodsId)

                if soNbl is None:
                    continue

                insertSql1 = """
                            insert into  cboss.i_user_radius
                            select t.so_nbr,t.seq,t.property_id,t.property_value from cboss.i_user_radius_his t
                            where t.commit_date < to_date('2022-01-15 00:00:00','yyyy-mm-dd hh24:mi:ss')
                                   and t.commit_date >= to_date('2022-01-14 00:00:00','yyyy-mm-dd hh24:mi:ss')
                                   and t.so_nbr = '{soNbl}'
                             """.format(soNbl=soNbl)
                ora.sqlExecute('cboss', insertSql1)

                insertSql2 = """
                             insert into cboss.i_user_radius_index
                                select * from cboss.i_user_radius_index_his a
                                where  a.process_date < to_date('2022-01-15 00:00:00','yyyy-mm-dd hh24:mi:ss')
                                       and a.process_date >= to_date('2022-01-14 00:00:00','yyyy-mm-dd hh24:mi:ss') 
                                       and a.busi_type = '83'
                                       and a.so_nbr = '{soNbl}'
                                       and a.bill_id = '{mobile}'
                             """ .format(soNbl=soNbl,mobile=mobile)
                ora .sqlExecute('cboss', insertSql2)

                updateSql = """
                            update bossmdy_jk1.shangxb_repair_20220119 t
                            set t.status = '1'
                            where t.mobile = '{mobile}'
                                  and t.goods_id = '{goodsId}'
                            """ .format(mobile=mobile,goodsId=goodsId)
                ora .sqlExecute('cboss',updateSql)
            except Exception as error:
                ora .dataRollback()
                logging .error(error)
            else:
                ora .dataCommit()

        logging .info(repairData)
    except Exception as error:
        logging .error(error)


if __name__ == '__main__':
    try:
        # 读取配置EquityRepairAutidNew
        config = Config('DefaultDataSourceCongfig')
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()

        # 加载新的数据源配置映射
        mapList = getDataSourceMapping()
        targetState = True

        exec()
    except Exception as error:
        logging .error(error)
    finally:
        if ora is not None:
            ora .dataClose()