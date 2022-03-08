# Time     2022/03/03 09::44
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ProgramConfigurationOnly import Config, OraclePolls

programCode = 'DefaultData'


def getData():
    try:
        selectSql = """
                    select * from  party.sms_message_receive a
                    where  a.create_date<sysdate-5/1440
                           and a.revc1 in('901911','901660','901910')
                    """
        selsctData = ora .selectSqlExecute('party', selectSql)
        return selsctData
    except Exception as error:
        logging .error(error)


def exce():
    try:
        dataList = getData()
        if len(dataList) == 0:
            logging .info('目前暂无数据需要处理')
            return

        # 数据转入错单
        ora .batchInsertAll('party', 'party.sms_message_receive_err', dataList)

        # 删除当前数据
        for data in dataList:
            id = data['ID']
            deleteSql = "delete from  party.sms_message_receive t where t.id = {id}" .format(id=str(id))
            ora .sqlExecute('party', deleteSql)

            logging .info('成功删除数据：' + str(id))

    except Exception as error:
        ora .dataRollback()
        logging .error(error)
    else:
        ora .dataCommit()


if __name__ == '__main__':
    try:
        # 读取配置EquityRepairAutidNew
        config = Config(programCode)
        staticConfig = config.getStaticConfig()
        logging = config.getLogging()
        ora = config.getDataSource()
        conDict = staticConfig.getKeyDict()

        exce()

    except Exception as error:
        logging .error(error)
    finally:
        if ora is not None:
            ora .dataClose()