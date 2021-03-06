# -*- coding:utf-8 -*-

# Time     2021/11/09 10::46
# Auhtor   ShangXb
# ide      PyCharm
# Verion   1.0
# function 权益订购稽核
from ToolTemplate import ToolTemplate
from MethodTools import getConfigFile
from RequestUtil import getRequestContent
from dateutil .relativedelta import relativedelta
from apscheduler .schedulers .blocking import BlockingScheduler
from apscheduler .triggers .cron import CronTrigger

import uuid
import datetime
import pandas as pd
import traceback

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


# 获取考产品核列表
def getProductData():
    productSql = """
                 select t.* from {environment}.order_info_product t
                 where t.state = 'U'
                 """ .format(environment=environment)
    productData = db .select('cboss', productSql)

    productDataList = [product['GOODSID'] for product in productData]
    return productDataList


# 获取用户映射
def getRegionUser():
    regionUserSql = "select * from {environment}.region_users t where t.state = 'U'" .format(environment=environment)
    regionUserDataList = db .select('cboss', regionUserSql)

    regionUserDict = {}
    for regionUserData in regionUserDataList:
        if regionUserData['SERVICE_NAME'] not in regionUserDict:
            regionUserDict[regionUserData['SERVICE_NAME']] = {}
        regionDict = {
            str(regionUserData['REGION_NAME']): regionUserData
        }
        regionUserDict[regionUserData['SERVICE_NAME']] .update(regionDict)
    return regionUserDict


# 获取中台接口返回的数据
def getRequestContext(mobile):
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
            "number": mobile
        }
        dataJsons = getRequestContent(url, data, headers)
        loggers .info(dataJsons)
        loggers.info(type(dataJsons))
        return dataJsons
    except Exception as error:
        raise error


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


# 获取订单数据
def getorderInfoSynTotalData():
    try:
        getDataSql = """
                     select a.orderid as order_Id,
                            a.suborderid as subOrder_Id,
                            a.virtual_prorelationid as Virtual_Prorelation_id,
                            a.real_prorelationid as Real_Prorelation_id,
                            a.post_prorelationid as Post_Prorelation_id,
                            a.result_code as Remote_Code,
                            a.result_code as Back_Code,
                            a.syn_result_code as Sync_Code,
                            a.status as Order_handle_Status,
                            a.post_status as Feed_back_Status,
                            a.productid as Product_id,
                            a.serviceno as mobile,
                            b.region_code as Region_Id,
                            a.createtime as Order_Create_Time,
                            a.updatetime as Order_Update_Time,
                            'U' as Audit_Status
                     from cboss.order_info_syn_total a left join cboss.GSM_HLR_INFO b on b.hlr_code = substr(a.serviceno,0,7)
                     where not exists(
                             select 1 from  {environment}.order_info_audit m
                             where a.orderid = m.order_id
                           )
                           and b.expire_date >= sysdate
                           and a.createtime < sysdate - 1/24
                           and a.createtime >= sysdate - 4
                     """ .format(environment=environment)
        orderData = db .select('cboss', getDataSql)

        if len(orderData) == 0:
            return

        db .insertMany('cboss', '{environment}.order_info_audit'.format(environment=environment), orderData)


    except Exception as error:
        db .rollback()
        loggers .error(error)
    else:
        db .commit()


# 获取需要稽核的订单数据
def getOrderInfoAuditData():
    getDataSql = """
                 select * from {environment}.order_info_audit t
                 where t.audit_status = 'U'
                       /*and t.order_id = 'SC19270T21110900016526'*/
                 """.format(environment=environment)
    auditData = db .select('cboss', getDataSql)
    return auditData


def orderAuditUpdate(auditResult):
    try:
        updateSql = """
                    update {environment}.order_info_audit t
                    set t.audit_desc = :auditDesc,
                        t.audit_code = :auditCode,
                        t.audit_status = :auditStaus,
                        t.audit_time = :auditTime,
                        t.assessment = :assessment
                    where t.order_id = :orderId
                    """.format(environment=environment)
        db .update('cboss', updateSql, auditResult)
    except Exception as error:
        loggers .error(error)
        db .rollback()
    else:
        db .commit()


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
    dbsourceName = regionUser['crm'][str(regionId)]['DBSOURCE_NAME']
    userName = regionUser['crm'][str(regionId)]['USER_NAME']

    orderUpdateTime = pd.to_datetime(orderUpdateTime, format='%Y-%m-%d %H:%M:%S')

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

    # 获取当前该策划的信息
    getInsOfferSql = getInsOfferSql .format(user=userName, region=str(regionId), offerInstId=OrdOfferData['OFFER_INST_ID'])
    insOfferData = db .select(dbsourceName, getInsOfferSql)

    if len(insOfferData) == 0:
        return None

    return insOfferData[0]


def getOrderDesc(mobile, orderId):
    # 调用中台的CIP00165接口查询当前中台的订单状态
    platformData = getRequestContext(mobile)
    # platformData = """{"respCode":"00000","respDesc":"success","result":{"bizCode":"0000","orderInfo":[{"amountReceivable":1990,"bpId":"2021999800037327","bpName":"手机QQ超级会员随心玩","channelId":"270","channelName":"中国移动集中运营中心","channelOrderId":"2111090420800101832-00","channelSubOrderId":"2111090420800101832-00","createTime":"20211109011009","drainageChannelId":"P00000008041","effectTime":"20211109011011","expireTime":"20991231000000","orderId":"SC19270T21110900016526","orderNum":"QD20211109011009259001","orderSource":"1","orderStatus":4,"orderType":"01","provinceRelationId":"65004843390721","shopCode":"Sa0000114","shopName":"百合计划-杭州卡赛","subOrderId":"SC19270T21110900016526-01","uniChannelId":"1000000002230300399"}],"totalNum":1}}"""
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

    return platformDict


def getRightsDesc(orderId):
    try:
        tfCaseData, tfAuditData = None, None
        # 获取订单数据
        getTfOrderdql = """
                        select * from tf_rc_order t
                        where t.out_open_order_id = '{orderId}'
                        """ .format(orderId=orderId)
        tfOrderData = db .select('rights', getTfOrderdql)

        if len(tfOrderData) != 0:
            # 获取
            order = tfOrderData[0]['ID']
            getTfCaseSql = """
                           select * from tf_rc_right_info_case t
                           where t.order_id  = '{order}'
                           """ .format(order=order)
            tfCaseData = db .select('rights', getTfCaseSql)

        getTfAudit = """
                     select * from tf_rc_province_relation_audit t
                     where OUT_OPEN_ORDER_ID = '{orderId}'
                     """ .format(orderId=orderId)
        tfAuditData = db .select('rights', getTfAudit)

        if len(tfAuditData) == 0:
            tfAuditData = None
        return tfCaseData, tfAuditData
    except Exception as error:
        raise error


def getAuditResult(auditCode=None, auditDesc=None, auditStaus=None, auditTime=None, orderId=None, assessment=None):
    auditResult = {
        'auditCode': '00' if auditCode is None else '99' if auditCode else '88',
        'auditDesc': '正常' if auditDesc is None else auditDesc,
        'auditStaus': 'E' if auditStaus is None else auditStaus,
        'auditTime': datetime .datetime .now() if auditTime is None else auditTime,
        'orderId': orderId,
        'assessment': '考核类产品' if assessment is not None and assessment else '非考核类产品'
    }
    return auditResult


def audit(auditData, platformOrder, crmOrder, rigthsOrder, rigthsAudit):
    # 判断权益中台是否存在相关的订单，如果存
    loggers .info(auditData)
    loggers.info(platformOrder)
    loggers.info(crmOrder)
    loggers.info(rigthsOrder)
    loggers.info(rigthsAudit)

    orderId = auditData['ORDER_ID']
    mobile = auditData['MOBILE']
    productId = auditData['PRODUCT_ID']
    assessment = False
    if productId in productData:
        assessment = True


    virtualProrelationId = auditData['VIRTUAL_PRORELATION_ID']

    # 存放稽核结果
    auditResult = None
    if platformOrder is None:
        auditDesc = "权益中台根据手机号码mobile[{mobile}]找不到对应的订单" \
                            .format(mobile=mobile)
        auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
        return auditResult

    # 中台订单不为空的情况
    # 获取中台订单订购状态和失效时间
    pfOrderStatusDesc = platformOrder['orderStatusDesc']
    pfExpireTimeStr = platformOrder['expireTime'] if 'expireTime' in platformOrder else None
    pfExpireTime = datetime.datetime.strptime(pfExpireTimeStr, '%Y%m%d%H%M%S') if pfExpireTimeStr is not None else None

    offerInstId = crmOrder['OFFER_INST_ID'] if crmOrder is not None else None
    nowTime = datetime .datetime .now()
    crmExpireDate = (pd.to_datetime(crmOrder['EXPIRE_DATE'], format='%Y-%m-%d %H:%M:%S')) if crmOrder is not None else None
    crmExpireDateStr = crmExpireDate .strftime('%Y%m%d%H%M%S') if crmExpireDate is not None else None
    # 判断订购状态和失效时间
    lastSecondTime, nextMonthTime = getLastTime(nowTime)

    if pfOrderStatusDesc == '已受理':
        auditDesc = "权益中台该订单状态为{pfOrderStatusDesc}" \
            .format(pfOrderStatusDesc=pfOrderStatusDesc)
        auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
        return auditResult

    if pfOrderStatusDesc in ['待支付', '订购超时', '支付失败']:
        if crmOrder is not None:
            auditDesc = "权益中台该订单状态为{pfOrderStatusDesc}，但省侧存在订购实例" \
                .format(pfOrderStatusDesc=pfOrderStatusDesc)
            auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
            return auditResult

    # 已订购、退订受理、退订失败、退订超时、退订中为中台存在参品
    if pfOrderStatusDesc in ['已订购', '退订受理', '退订失败', '退订超时', '退订中'] and assessment:
        if crmOrder is None:
            auditDesc = "权益中台该订单为已订购，但省侧并无对应的订购实例" \
                .format(offerInstId=offerInstId, crmExpireDateStr=crmExpireDateStr)
            auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
            return auditResult

        crmOrderStatusDesc = '已退订' if crmOrder is not None else None
        if crmExpireDate is not None and crmExpireDate > nextMonthTime:
            crmOrderStatusDesc = '已订购'
        if crmOrderStatusDesc != '已订购':
            auditDesc = "权益中台和省侧状态不一致，省侧为{crmOrderStatusDesc}，中台为{pfOrderStatusDesc}" \
                .format(crmOrderStatusDesc=crmOrderStatusDesc, pfOrderStatusDesc=pfOrderStatusDesc)
            auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
            return auditResult
        if pfExpireTimeStr != crmExpireDateStr:
            auditDesc = "权益中台和省侧有效时间不一致，省侧为{crmExpireDateStr}，中台为{pfExpireTimeStr}" \
                .format(crmExpireDateStr=crmExpireDateStr, pfExpireTimeStr=pfExpireTimeStr)
            auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
            return auditResult

        # 已订购、退订受理、退订失败、退订超时、退订中为中台存在参品
        if pfOrderStatusDesc in ['已订购', '退订受理', '退订失败', '退订超时', '退订中'] and not assessment:
            if crmOrder is None:
                auditDesc = "权益中台该订单为已订购，但省侧并无对应的订购实例" \
                    .format(offerInstId=offerInstId, crmExpireDateStr=crmExpireDateStr)
                auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId,
                                             assessment=assessment)
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
            auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
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
    if rigthsOrder is None:
        auditDesc = "权益中台、营业侧均有此订单数据，但权益中心无相关的数据"
        auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
        return auditResult

    # 判断是否保障的数据
    if virtualProrelationId is not None and rigthsAudit is None:
        auditDesc = "次数据为保障数据，但保障的策划实例编号未同步权益中心"
        auditResult = getAuditResult(auditCode=assessment, auditDesc=auditDesc, auditStaus='E', orderId=orderId, assessment=assessment)
        return auditResult

    return getAuditResult(orderId=orderId, assessment=assessment)


def exce():
    try:
        global tool, db, loggers, configDict, regionUser, productData
        tool = ToolTemplate(dataSourctName=programName, logginFileName=loggingFileName)
        db = tool.getDbSourct()
        loggers = tool.getLoggers()
        configDict = tool.getConfigDict()

        # 切换日志级别
        loggers.setLogger('info')
        getorderInfoSynTotalData()
        auditDataList = getOrderInfoAuditData()

        regionUser = getRegionUser()
        productData = getProductData()

        for auditData in auditDataList:
            try:
                orderId = auditData['ORDER_ID']                             # 能开订单编号
                suborderId = auditData['SUBORDER_ID']                       # 能开子订单编号
                virtualProrelationId = auditData['VIRTUAL_PRORELATION_ID']  # 省侧虚拟订购实例编号
                realProrelationId = auditData['REAL_PRORELATION_ID']        # 省侧真实订购实例编号
                postProrelationId = auditData['POST_PRORELATION_ID']        # 65返回的实例编号
                remoteCode = auditData['REMOTE_CODE']                       # remote返回码
                backCode = auditData['BACK_CODE']                           # 65返回码
                syncCode = auditData['SYNC_CODE']                           # 同步权益返回码
                orderHandleStatus = auditData['ORDER_HANDLE_STATUS']        # 订单处理状态
                feedBackStatus = auditData['FEED_BACK_STATUS']              # 65返回状态
                orderCreateTime = auditData['ORDER_CREATE_TIME']            # 订单创建时间
                orderUpdateTime = auditData['ORDER_UPDATE_TIME']            # 订单重做时间
                productId = auditData['PRODUCT_ID']                         # 产品编号
                mobile = auditData['MOBILE']                                # 手机号码
                regionId = auditData['REGION_ID']                           # 地市

                # 存放稽核结果明细
                auditResultDesc = None

                # 获取中台数据
                platformOrder = getOrderDesc(mobile, orderId)

                # 获取营业侧的订购实例
                crmOrder = getInsDesc(orderId, regionId, orderUpdateTime)

                # 获取权益中心数据
                rigthsOrder, rigthsAuditData = getRightsDesc(orderId)

                auditResultDesc = audit(auditData, platformOrder, crmOrder, rigthsOrder, rigthsAuditData)

                loggers .info(auditResultDesc)

                orderAuditUpdate(auditResultDesc)

            except Exception as error:
                loggers .error(error)
                loggers .error(traceback .format_exc())

            finally:
                loggers .info('必定执行')
    except Exception as error:
        loggers .error(error)
        loggers.error(traceback.format_exc())
    finally:
        db .close()


if __name__ == '__main__':
    # configFilepath = 'E:/程序/pyhton/shangxb/configStatic/RightsOrderAutidConfig'
    configFilepath = '/app/cbapp/program/config/RightsOrderAutidConfig'
    configData = getConfigFile(configFilepath)

    programName = dict(configData.items('PROGRAM'))['programName']
    loggingFileName = dict(configData.items('LOGGING'))['fileName']
    crontab = dict(configData.items('CRONTAB'))['crontab']
    environment = dict(configData.items('CONFIG'))['environment']
    timing = dict(configData.items('CONFIG'))['timing']

    tool = None
    db = None
    loggers = None
    configDict = None


    regionUser = None
    productData = None

    # 判断是否开启执行器
    if timing == 'true':
        scheduler = BlockingScheduler()
        scheduler .add_job(exce, CronTrigger .from_crontab(crontab))
        scheduler .start()
    else:
        exce()