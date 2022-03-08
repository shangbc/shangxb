# Time     2021/11/25 15::10
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
from ToolTemplate import ToolTemplate
from MethodTools import getConfigFile
from RequestUtil import getRequestContent
from dateutil.relativedelta import relativedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

import uuid
import datetime
import pandas as pd
import traceback
import re

# 中台订购状态映射
orderStatusMap = {
    '0': '已受理',
    '1': '待支付',
    '2': '已订购',
    '3': '退订受理',
    '4': '已退订',
    '5': '退订失败',
    '6': '退费受理',
    '7': '已退费',
    '8': '退费异常',
    '9': '退费驳回',
    '10': '支付失败',
    '11': '已失效',
    '12': '订购超时',
    '13': '退订超时',
    '14': '订购失败',
    '15': '退订中',
    '17': '已终止'
}

# 获取指定月份的最后一天最后一秒的时间和下月最开始的时间
def getLastTime(afferentTime):
    year = afferentTime .year
    month = afferentTime .month
    if month == 12:
        year += 1
        month = 1
    else:
        month += 1
    nextMonthTime = datetime .datetime(year=year, month=month, day=1)
    lastSecondTime = nextMonthTime - datetime .timedelta(seconds=1)
    return lastSecondTime, nextMonthTime


# 获取考产品核列表
def getProductData():
    productSql = """
                 select t.* from {environment}.order_info_product t
                 where t.state = 'U'
                 """.format(environment='cboss' if environment == 'test' else 'bossmdy')
    productData = db.select('cboss', productSql)

    productDataList = [product['GOODSID'] for product in productData]
    return productDataList


# 获取用户映射
def getRegionUser():
    regionUserSql = "select * from {environment}.region_users t where t.state = 'U'" \
        .format(environment='cboss' if environment == 'test' else 'bossmdy')
    regionUserDataList = db.select('cboss', regionUserSql)

    regionUserDict = {}
    for regionUserData in regionUserDataList:
        if regionUserData['SERVICE_NAME'] not in regionUserDict:
            regionUserDict[regionUserData['SERVICE_NAME']] = {}
        regionDict = {
            str(regionUserData['REGION_NAME']): regionUserData
        }
        regionUserDict[regionUserData['SERVICE_NAME']].update(regionDict)
    return regionUserDict


def getPlatformOrder():
    try:
        # 只取考核类产品
        productDataList = "'" + "','".join(productData) + "'"

        # 跨月数据获取，每月1号获取上月最后一天的数据
        timeStr = ''
        nowTime = datetime.datetime.now()
        exceTime = None
        day = nowTime.day
        if day == 1:
            exceTime = nowTime - datetime.timedelta(days=1)
        else:
            exceTime = nowTime
        timeStr = exceTime.strftime('%Y%m')

        orderSql = """
                   select t.id as UnsubscribeId,
                           t.platform_order_code,
                           substr(t.ext1,
                                  instr(t.ext1, '"GOODSTITLE":"', 1, 1) +
                                  length('"GOODSTITLE":"'),
                                  instr(t.ext1, '","RETURNQUANTITY"', 1, 1) -
                                  instr(t.ext1, '"GOODSTITLE":"', 1, 1) -
                                  length('"GOODSTITLE":"')) as goodsname,
                           substr(t.ext1,
                                  instr(t.ext1, '"GOODSID":"', 1, 1) + length('"GOODSID":"'),
                                  instr(t.ext1, '","PRICE":"', 1, 1) -
                                  instr(t.ext1, '"GOODSID":"', 1, 1) - length('"GOODSID":"')) as goodsid,
                           substr(t.ext1,
                                  instr(t.ext1, '"SERVICENO":"', 1, 1) + length('"SERVICENO":"'),
                                  instr(t.ext1, '","SERVICENOTYPE"', 1, 1) -
                                  instr(t.ext1, '"SERVICENO":"', 1, 1) - length('"SERVICENO":"')) as mobile
                      from {environment}.platform_order_{timeStr} t
                     where substr(t.ext1,
                                  instr(t.ext1, '"GOODSID":"', 1, 1) + length('"GOODSID":"'),
                                  instr(t.ext1, '","PRICE":"', 1, 1) -
                                  instr(t.ext1, '"GOODSID":"', 1, 1) - length('"GOODSID":"')) in
                           ({productDataList})
                       and t.order_state <> '00'
                       and t.order_type = 7
                       and t.create_date >= sysdate - 3
                   """.format(environment='cboss' if environment == 'test' else 'mboss',
                              productDataList=productDataList,
                              timeStr=timeStr)
        platformOrderData = db.select('cboss' if environment == 'test' else 'mboss', orderSql)

        if len(platformOrderData) == 0:
            loggers.info('当前从订单表中没有获取到数据')
            return

        deleteSql = "delete from {environment}.Rights_Unsubscribe_temporary" \
            .format(environment='cboss' if environment == 'test' else 'bossmdy')
        db.delete('cboss', deleteSql)

        db.insertMany('cboss',
                      '{environment}.Rights_Unsubscribe_temporary'.format(
                          environment='cboss' if environment == 'test' else 'bossmdy'),
                      platformOrderData)

        insertSql = """
                    select a.unsubscribeid,
                           a.platform_order_code,
                           a.goodsname,
                           a.goodsid,
                           a.mobile,
                           b.region_code as regionid,
                           sysdate as create_time,
                           sysdate as autid_time,
                           'U' as autid_state,
                           a.autid_code,
                           a.autid_desc
                    from {environment1}.Rights_Unsubscribe_temporary a left join cboss.GSM_HLR_INFO b on b.hlr_code = substr(a.mobile,0,7)
                    where b.expire_date > sysdate
                          and not exists(
                              select 1 from {environment2}.rights_unsubscribe_autid m
                              where a.unsubscribeid = m.unsubscribeid
                          )
                    """ .format(environment1='cboss' if environment == 'test' else 'bossmdy',
                                environment2='cboss' if environment == 'test' else 'bossmdy')
        insertData = db.select('cboss', insertSql)
        db.insertMany('cboss',
                      '{environment}.Rights_Unsubscribe_Autid'.format(
                          environment='cboss' if environment == 'test' else 'bossmdy'),
                      insertData)


    except Exception as error:
        db.rollback()
        loggers.error(error)
    else:
        db.commit()


def getAutidData():
    getDataSql = """
                 select * from {environment}.Rights_Unsubscribe_Autid t
                 where t.autid_state = 'U'
                 """ .format(environment='cboss' if environment == 'test' else 'bossmdy')
    dataList = db.select('cboss', getDataSql)
    return dataList


# 获取中台接口返回的数据
def getRequestContext(mobile, startDate=None, endDate=None):
    try:
        messageId = str(uuid.uuid1()).replace("-", "")

        timeStamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

        headers = {
            "domain": "CMCOP",
            "routevalue": "998",
            "version": "v1",
            "sign": "571",
            "appid": "571",
            "APICode": "CIP00165",
            "messageId": messageId,
            "timeStamp": timeStamp
        }

        url = "http://10.254.50.252:3911/v3"
        # data = {
        #     "endDate": "",
        #     "startDate": "",
        #     "pageSize": 10,
        #     "pageNum": 1,
        #     "orderType": "",
        #     "bpId": "",
        #     "number": "13587851710"
        # }
        data = {
            "endDate": endDate,
            "startDate": startDate,
            "number": mobile
        }
        dataJsons = getRequestContent(url, data, headers)
        loggers .info(dataJsons)
        loggers.info(type(dataJsons))
        return dataJsons
    except Exception as error:
        raise error


def getOrderDesc(mobile, orderId, crmOrder):
    # 调用中台的CIP00165接口查询当前中台的订单状态
    platformData = getRequestContext(mobile)
    # platformData = """{"respCode":"00000","respDesc":"success","result":{"bizCode":"0000","orderInfo":[{"amountReceivable":1500,"bpId":"9990400100001320001","bpName":"随心选会员","channelId":"959","channelName":"浙江移动","channelOrderId":"64004958237136","channelSubOrderId":"","createTime":"20210915193906","effectTime":"20200520142340","expireTime":"20211231235959","orderId":"64004958237136","orderNum":"HQ20210915193906526001","orderSource":"2","orderStatus":2,"orderType":"01","provinceRelationId":"64004958237136"}],"totalNum":1}}"""
    # platformData = eval(platformData)
    platformDataList = platformData['result']['orderInfo'] if 'result' in platformData else []

    # 遍历中台查询到的订单信息
    platformDict = None
    for platform in platformDataList:
        pfOrderId = platform['orderId'] if 'orderId' in platform else None  # 订单号
        pfSubOrderId = platform['subOrderId'] if 'subOrderId' in platform else None  # 订单子编码
        pfEffectTime = platform['effectTime'] if 'effectTime' in platform else None  # 生效时间
        pfExpireTime = platform['expireTime'] if 'expireTime' in platform else None  # 失效时间
        pfCreateTime = platform['createTime'] if 'createTime' in platform else None  # 下单时间
        pfBpId = platform['bpId'] if 'bpId' in platform else None  # 权益包能开商品编码
        pfOrderStatus = platform['orderStatus'] if 'orderStatus' in platform else None # 订单状态
        pfProvinceRelationId = platform['provinceRelationId'] if 'provinceRelationId' in platform else None
        orderStatusDesc = orderStatusMap[str(pfOrderStatus)]
        if orderId == pfOrderId:
            platformDict = {
                'orderId': pfOrderId,
                'subOrderId': pfSubOrderId,
                'effectTime': pfEffectTime,
                'expireTime': pfExpireTime,
                'createTime': pfCreateTime,
                'bpId': pfBpId,
                'orderStatus': pfOrderStatus,
                'provinceRelationId': pfProvinceRelationId,
                'orderStatusDesc': orderStatusDesc
            }
            break
        elif pfProvinceRelationId == orderId:
            platformDict = {
                'orderId': pfOrderId,
                'subOrderId': pfSubOrderId,
                'effectTime': pfEffectTime,
                'expireTime': pfExpireTime,
                'createTime': pfCreateTime,
                'bpId': pfBpId,
                'orderStatus': pfOrderStatus,
                'provinceRelationId': pfProvinceRelationId,
                'orderStatusDesc': orderStatusDesc
            }
            break

    if platformDict is None and crmOrder is not None:
        createTime = pd.to_datetime(crmOrder['CREATE_DATE'], '%Y/%m/%d %H:%M:%S')
        nowDate = datetime.datetime.now()
        createTimeStr = createTime.strftime('%Y%m%d')
        nowDateTimeStr = nowDate.strftime('%Y%m%d')
        platformData = getRequestContext(mobile, createTimeStr, nowDateTimeStr)
        platformDataList = platformData['result']['orderInfo'] if 'result' in platformData else []
        insOfferId = str(int(crmOrder['OFFER_INST_ID']))

        for platform in platformDataList:
            pfOrderId = platform['orderId'] if 'orderId' in platform else None  # 订单号
            pfSubOrderId = platform['subOrderId'] if 'subOrderId' in platform else None  # 订单子编码
            pfEffectTime = platform['effectTime'] if 'effectTime' in platform else None  # 生效时间
            pfExpireTime = platform['expireTime'] if 'expireTime' in platform else None  # 失效时间
            pfCreateTime = platform['createTime'] if 'createTime' in platform else None  # 下单时间
            pfBpId = platform['bpId'] if 'bpId' in platform else None  # 权益包能开商品编码
            pfOrderStatus = platform['orderStatus'] if 'orderStatus' in platform else None  # 订单状态
            pfProvinceRelationId = platform['provinceRelationId'] if 'provinceRelationId' in platform else None
            orderStatusDesc = orderStatusMap[str(pfOrderStatus)]
            if orderId == pfOrderId:
                platformDict = {
                    'orderId': pfOrderId,
                    'subOrderId': pfSubOrderId,
                    'effectTime': pfEffectTime,
                    'expireTime': pfExpireTime,
                    'createTime': pfCreateTime,
                    'bpId': pfBpId,
                    'orderStatus': pfOrderStatus,
                    'provinceRelationId': pfProvinceRelationId,
                    'orderStatusDesc': orderStatusDesc
                }
                break
            elif pfProvinceRelationId == orderId:
                platformDict = {
                    'orderId': pfOrderId,
                    'subOrderId': pfSubOrderId,
                    'effectTime': pfEffectTime,
                    'expireTime': pfExpireTime,
                    'createTime': pfCreateTime,
                    'bpId': pfBpId,
                    'orderStatus': pfOrderStatus,
                    'provinceRelationId': pfProvinceRelationId,
                    'orderStatusDesc': orderStatusDesc
                }
                break
            elif insOfferId == pfOrderId:
                platformDict = {
                    'orderId': pfOrderId,
                    'subOrderId': pfSubOrderId,
                    'effectTime': pfEffectTime,
                    'expireTime': pfExpireTime,
                    'createTime': pfCreateTime,
                    'bpId': pfBpId,
                    'orderStatus': pfOrderStatus,
                    'provinceRelationId': pfProvinceRelationId,
                    'orderStatusDesc': orderStatusDesc
                }
                break

    return platformDict

# 检索业务办理时间的本月、上一个月和下一个月，避免跨月导致的误差
def monthOperation(dbsourceName, sql, updateTime):
    monthOperation = [0, 1, -1]
    data = None
    for monthIndex in monthOperation:
        try:
            timeStr = updateTime + relativedelta(months=monthIndex)
            timeStr = timeStr .strftime('%Y%m')
            selectSql = sql .format(timeStr=timeStr)
            dataList = db .select(dbsourceName, selectSql)

            if len(dataList) == 0:
                continue

            data = dataList[0]
            break
        except Exception as error:
            loggers .error(error)
            loggers.error(traceback.format_exc())
            continue

    return data


# 获取省侧订购实例
def getInsDesc(order, regionId, orderUpdateTime):
    # 获取对应的数据源和用户
    dbsourceName = regionUser['crm'][str(regionId)]['DBSOURCE_NAME']
    userName = regionUser['crm'][str(regionId)]['USER_NAME']

    # orderUpdateTime = pd.to_datetime(orderUpdateTime, format='%Y-%m-%d %H:%M:%S')

    # 判断退订订单的类型是否为能开变化退订
    orderCompile = re .compile("^(SC).*")
    nkBoolen = orderCompile .search(order)

    getSoBusiSql = """
                   select t.* from {user}.so_busi_ext_{region}_{timeStr} t
                   where t.busi_type = 'abilityOrderID'
                         and t.busi_code = '{order}'
                   """

    getOrdOfferSql = """
                     select t.* from {user}.ord_offer_f_{region}_{timeStr} t
                     where t.offer_type = 'OFFER_VAS_OTHER'
                        and t.customer_order_id = '{customerOrderId}'
                     """

    getInsOfferSql = """
                     select t.* from {user}.ins_offer_{region} t
                     where t.offer_inst_id = '{offerInstId}'
                     """

    # 根据是否为能开订单编号选择退订的方式
    offerInstId = None
    if nkBoolen is not None:
        orderTimeStr = '20' + order[8:12]
        loggers.info(orderTimeStr)
        orderUpdateTime = datetime.datetime.strptime(orderTimeStr, '%Y%m')

        # 获取订单信息
        getSoBusiSql = getSoBusiSql .format(user=userName, region=str(regionId), timeStr='{timeStr}', order=order)
        soBusiData = monthOperation(dbsourceName, getSoBusiSql, orderUpdateTime)
        if soBusiData is None:
            return None

        # 获取省侧订购实例编号
        getOrdOfferSql = getOrdOfferSql .format(user=userName, region=str(regionId), timeStr='{timeStr}',
                                          customerOrderId=str(soBusiData['CUSTOMER_ORDER_ID']))
        OrdOfferData = monthOperation(dbsourceName, getOrdOfferSql, orderUpdateTime)
        if OrdOfferData is None:
            return None
        offerInstId = OrdOfferData['OFFER_INST_ID']
    else:
        offerInstId = order

    # 获取当前该策划的信息
    getInsOfferSql = getInsOfferSql .format(user=userName, region=str(regionId), offerInstId=offerInstId)
    insOfferData = db .select(dbsourceName, getInsOfferSql)

    if len(insOfferData) == 0:
        return None

    return insOfferData[0]


def getAuditResult(auditCode=None, auditDesc=None, auditStaus=None, auditTime=None, orderId=None):
    auditResult = {
        'auditCode': '00' if auditCode is None else '99' if auditCode else '88',
        'auditDesc': '正常' if auditDesc is None else auditDesc,
        'auditStaus': 'E' if auditStaus is None else auditStaus,
        'auditTime': datetime .datetime .now() if auditTime is None else auditTime,
        'unsubscribeid': orderId,
    }
    return auditResult


def audit(auditData, platformOrder, crmOrder):
    # 判断权益中台是否存在相关的订单，如果存
    loggers .info("订单信息：" + auditData .__str__())
    loggers.info("中台订单：" + platformOrder .__str__())
    loggers.info("营业侧订单：" + crmOrder .__str__())

    orderId = auditData['PLATFORM_ORDER_CODE']
    unsubscribeid = auditData['UNSUBSCRIBEID']
    mobile = auditData['MOBILE']
    productId = auditData['goodsid']

    crmExpireDate = (
        pd.to_datetime(crmOrder['EXPIRE_DATE'], format='%Y-%m-%d %H:%M:%S')) if crmOrder is not None else None
    crmExpireDateStr = crmExpireDate.strftime('%Y%m%d%H%M%S') if crmExpireDate is not None else None
    nowTime = datetime .datetime .now()
    # 判断订购状态和失效时间
    lastSecondTime, nextMonthTime = getLastTime(nowTime)

    # 存放稽核结果
    auditResult = None
    if platformOrder is None:
        if crmOrder is None:
            return getAuditResult(orderId=unsubscribeid)

        crmOrderStatusDesc = '已退订' if crmOrder is not None else None
        if crmExpireDate is not None and crmExpireDate > nextMonthTime:
            crmOrderStatusDesc = '已订购'
        if crmOrderStatusDesc != '已退订':
            auditDesc = "权益中台和省侧状态不一致，省侧为{crmOrderStatusDesc}，中台为已退订" \
                .format(crmOrderStatusDesc=crmOrderStatusDesc)
            auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='U', orderId=unsubscribeid)
            return auditResult

        return getAuditResult(orderId=unsubscribeid)



    # 中台订单不为空的情况
    # 获取中台订单订购状态和失效时间
    pfOrderStatusDesc = platformOrder['orderStatusDesc']
    pfExpireTimeStr = platformOrder['expireTime'] if 'expireTime' in platformOrder else None
    pfExpireTime = datetime.datetime.strptime(pfExpireTimeStr, '%Y%m%d%H%M%S') if pfExpireTimeStr is not None else None

    offerInstId = crmOrder['OFFER_INST_ID'] if crmOrder is not None else None

    if pfOrderStatusDesc == '已受理':
        auditDesc = "权益中台该订单状态为{pfOrderStatusDesc}" \
            .format(pfOrderStatusDesc=pfOrderStatusDesc)
        auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='U', orderId=unsubscribeid)
        return auditResult

    if pfOrderStatusDesc in ['待支付', '订购超时', '支付失败']:
        if crmOrder is not None:
            auditDesc = "权益中台该订单状态为{pfOrderStatusDesc}，但省侧存在订购实例" \
                .format(pfOrderStatusDesc=pfOrderStatusDesc)
            auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='U', orderId=unsubscribeid)
            return auditResult

    # 已订购、退订受理、退订失败、退订超时、退订中为中台存在参品
    if pfOrderStatusDesc in ['已订购', '退订受理', '退订失败', '退订超时', '退订中']:
        if crmOrder is None:
            auditDesc = "权益中台该订单为已订购，但省侧并无对应的订购实例" \
                .format(offerInstId=offerInstId, crmExpireDateStr=crmExpireDateStr)
            auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='U', orderId=unsubscribeid)
            return auditResult

        crmOrderStatusDesc = '已退订' if crmOrder is not None else None
        if crmExpireDate is not None and crmExpireDate > nextMonthTime:
            crmOrderStatusDesc = '已订购'
        if crmOrderStatusDesc != '已订购':
            auditDesc = "权益中台和省侧状态不一致，省侧为{crmOrderStatusDesc}，中台为{pfOrderStatusDesc}" \
                .format(crmOrderStatusDesc=crmOrderStatusDesc, pfOrderStatusDesc=pfOrderStatusDesc)
            auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='U', orderId=unsubscribeid)
            return auditResult
        if pfExpireTimeStr != crmExpireDateStr:
            auditDesc = "权益中台和省侧有效时间不一致，省侧为{crmExpireDateStr}，中台为{pfExpireTimeStr}" \
                .format(crmExpireDateStr=crmExpireDateStr, pfExpireTimeStr=pfExpireTimeStr)
            auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='U', orderId=unsubscribeid)
            return auditResult

    # 已退订状态
    if pfOrderStatusDesc == '已退订':
        crmOrderStatusDesc = '已订购' if crmOrder is not None else None

        # 为了覆盖灵活账期，省侧生效时间小于等于次月月底即可
        if crmExpireDate is not None and (
                crmExpireDate <= nowTime or crmExpireDate <= nextMonthTime + relativedelta(months=1)):
            crmOrderStatusDesc = '已退订'
        if crmOrder is not None and crmOrderStatusDesc != '已退订':
            auditDesc = "权益中台和省侧状态不一致，省侧为{crmOrderStatusDesc}，中台为{pfOrderStatusDesc}" \
                .format(crmOrderStatusDesc=crmOrderStatusDesc, pfOrderStatusDesc=pfOrderStatusDesc)
            auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='U', orderId=unsubscribeid)
            return auditResult

        # 已退订不校验失效时间
        # if crmOrder is not None and pfExpireTimeStr != crmExpireDateStr:
        #     auditDesc = "权益中台和省侧失效时间不一致，省侧为{crmExpireDateStr}，中台为{pfExpireTimeStr}" \
        #         .format(crmExpireDateStr=crmExpireDateStr, pfExpireTimeStr=pfExpireTimeStr)
        #     auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='E', orderId=orderId)
        #     return auditResult

    # # 中台订单为空的情况
    # if platformOrder is None:
    #     if crmOrder is not None:
    #         crmExpireDate = pd.to_datetime(crmOrder['EXPIRE_DATE'], format='%Y-%m-%d %H:%M:%S')
    #         if crmExpireDate > datetime .datetime .now():
    #             # 异常的情况
    #             auditDesc = "权益中台根据手机号码mobile[{mobile}]找不到对应的订单,但省侧存在对应的策划实例offerInstId[{offerInstId}]" \
    #                 .format(mobile=mobile, offerInstId=offerInstId)
    #             auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='E', orderId=orderId)
    #             return auditResult
    #         else:
    #             crmExpireDateStr = crmExpireDate .strftime('%Y-%m-%d %H:%M:%S')
    #             auditDesc = "省侧存在对应的策划实例offerInstId[{offerInstId}],但其失效时间为{crmExpireDateStr}" \
    #                 .format(offerInstId=offerInstId, crmExpireDateStr=crmExpireDateStr)
    #             auditResult = getAuditResult(auditDesc=auditDesc)
    #             return auditResult
    #
    #     else:
    #         if rigthsOrder is not None:
    #             auditDesc = "权益中台、营业侧均无此订单数据，但权益中心有相关的数据"
    #             auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='E', orderId=orderId)
    #             return auditResult
    #         else:
    #             return getAuditResult()
    #
    # # 中台订单不为空的情况
    # # 获取中台订单订购状态和失效时间
    # pfOrderStatusDesc = platformOrder['orderStatusDesc']
    # pfExpireTimeStr = platformOrder['expireTime'] if 'expireTime' in platformOrder else None
    #
    # if crmOrder is None:
    #     # 异常的情况
    #     # 订购未走完的流程
    #     if pfOrderStatusDesc == '待支付':
    #         auditDesc = "中台" \
    #             .format(mobile=mobile)
    #         auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='E', orderId=orderId)
    #         return auditResult
    #     auditDesc = "权益中台根据手机号码mobile[{mobile}]找到对应的订单,但省侧不存在对应的策划实例" \
    #         .format(mobile=mobile)
    #     auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='E', orderId=orderId)
    #     return auditResult
    # else:
    #     crmOrderStatusDesc = '已订购'
    #     nowTime = datetime .datetime .now()
    #     crmExpireDate = pd.to_datetime(crmOrder['EXPIRE_DATE'], format='%Y-%m-%d %H:%M:%S')
    #     crmExpireDateStr = crmExpireDate .strftime('%Y%m%d%H%M%S')
    #     pfExpireTime = datetime .datetime .strptime(pfExpireTimeStr, '%Y%m%d%H%M%S') if pfExpireTimeStr is not None else None
    #     # 判断订购状态和失效时间
    #     lastSecondTime, nextMonthTime = getLastTime(nowTime)
    #
    #     loggers .info(crmExpireDate)
    #     loggers .info(nextMonthTime)
    #     loggers .info(lastSecondTime)
    #     # 判断省侧的订购状态
    #     if crmExpireDate <= nowTime or crmExpireDate == nextMonthTime or crmExpireDate == lastSecondTime:
    #         crmOrderStatusDesc = '已退订'
    #
    #     if pfOrderStatusDesc != crmOrderStatusDesc:
    #
    #         auditDesc = "省侧和中台订购状态不一致，省侧为{crmOrderStatusDesc}，而中台的状态为{pfOrderStatusDesc}" \
    #             .format(crmOrderStatusDesc=crmOrderStatusDesc, pfOrderStatusDesc=pfOrderStatusDesc)
    #         auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='E', orderId=orderId)
    #         return auditResult
    #
    #     if pfExpireTimeStr != crmExpireDateStr:
    #         auditDesc = "省侧和中台有效时间不一致，省侧失效时间为{crmExpireDateStr}，中台失效时间为{pfExpireTimeStr}" \
    #             .format(crmExpireDateStr=crmExpireDateStr, pfExpireTimeStr=pfExpireTimeStr)
    #         auditResult = getAuditResult(auditCode='99', auditDesc=auditDesc, auditStaus='E', orderId=orderId)
    #         return auditResult
    #
    # 权益中心
    # if rigthsOrder is None:
    #     auditDesc = "权益中台、营业侧均有此订单数据，但权益中心无相关的数据"
    #     auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
    #     return auditResult
    #
    # # 判断是否保障的数据
    # if virtualProrelationId is not None and rigthsAudit is None:
    #     auditDesc = "次数据为保障数据，但保障的策划实例编号未同步权益中心"
    #     auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
    #    return auditResult

    return getAuditResult(orderId=unsubscribeid)


def orderAuditUpdate(auditResult):
    try:
        updateSql = """
                    update {environment}.rights_unsubscribe_autid t
                    set t.autid_desc = :auditDesc,
                        t.autid_code = :auditCode,
                        t.autid_state = :auditStaus,
                        t.autid_time = :auditTime
                    where t.unsubscribeid = :unsubscribeid
                    """.format(environment='cboss' if environment == 'test' else 'bossmdy')
        db .update('cboss', updateSql, auditResult)
    except Exception as error:
        loggers .error(error)
        db .rollback()
    else:
        db .commit()


def exce():
    try:
        global tool, db, loggers, configDict, regionUser, productData, level
        tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
        db = tool.getDbSourct()
        loggers = tool.getLoggers()
        configDict = tool.getConfigDict()

        # 切换日志级别
        loggers.setLogger(level)

        # 获取用户映射
        regionUser = getRegionUser()

        # 获取考核的产品列表
        productData = getProductData()

        # 获取需要处理的数据
        getPlatformOrder()
        auditDataList = getAutidData()

        for auditData in auditDataList:
            try:
                mobile = auditData['mobile']
                platformOrderCode = auditData['PLATFORM_ORDER_CODE']
                regionId = auditData['REGIONID']
                createTime = auditData['CREATE_TIME']
                loggers.info(auditData)

                # 获取营业侧的订购实例
                crmOrder = getInsDesc(platformOrderCode, regionId, createTime)

                # 获取中台数据
                platformOrder = getOrderDesc(mobile, platformOrderCode, crmOrder)

                # 对比省侧和中台的订购关系
                auditResultDesc = audit(auditData, platformOrder, crmOrder)

                loggers.info(auditResultDesc)

                orderAuditUpdate(auditResultDesc)
            except Exception as error:
                loggers .error(error)
    except Exception as error:
        loggers .error(error)


if __name__ == '__main__':
    # configFilepath = 'E:/程序/pyhton/shangxb/configStatic/RightsUnsubscribeAutidConfig'
    configFilepath = '/app/cbapp/program/config/RightsUnsubscribeAutidConfig'
    configData = getConfigFile(configFilepath)

    programName = dict(configData.items('PROGRAM'))['programName']
    loggingFileName = dict(configData.items('LOGGING'))['fileName']
    crontab = dict(configData.items('CRONTAB'))['crontab']
    environment = dict(configData.items('CONFIG'))['environment']
    timing = dict(configData.items('CONFIG'))['timing']
    level = dict(configData.items('CONFIG'))['level']

    tool = None
    db = None
    loggers = None
    configDict = None


    regionUser = None
    productData = None

    # 判断是否开启执行器
    if timing == 'true':
        scheduler = BlockingScheduler()
        scheduler.add_job(exce, CronTrigger.from_crontab(crontab))
        scheduler.start()
    else:
        exce()
