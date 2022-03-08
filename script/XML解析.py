# Time     2022/02/25 15::14
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认

from xml.dom.minidom import parse
import xml.dom.minidom
import os
import re

fileList = None
for root, dirs, files in os.walk("C:\\Users\\Lenovo\\Desktop\\新建文件夹"):
    fileList = files


fileNameList = ['ftp_config_10_5102.xml','ftp_5_56.xml','ftp_6_56.xml','ftp_conf_vces_6711.xml','ftp_config_0.xml','ftp_config_10.xml','ftp_config_10_kh.xml','ftp_config_100.xml','ftp_config_1001.xml','ftp_config_10013.xml','ftp_config_1003.xml','ftp_config_101.xml','ftp_config_1022.xml','ftp_config_1160.xml','ftp_config_1161.xml','ftp_config_1162.xml','ftp_config_1163.xml','ftp_config_1167.xml','ftp_config_1168.xml','ftp_config_180.xml','ftp_config_200_201.xml','ftp_config_21_301.xml','ftp_config_333.xml','ftp_config_3999.xml','ftp_config_4000.xml','ftp_config_41.xml','ftp_config_4204.xml','ftp_config_436.xml','ftp_config_44.xml','ftp_config_44_2.xml','ftp_config_5015.xml','ftp_config_51.xml','ftp_config_541.xml','ftp_config_5411.xml','ftp_config_542.xml','ftp_config_58.xml','ftp_config_66_56.xml','ftp_config_8.xml','ftp_config_81801.xml','ftp_config_81802.xml','ftp_config_81803.xml','ftp_config_9.xml','ftp_config_90.xml','ftp_config_90_1.xml','ftp_config_90_2.xml','ftp_config_90_3.xml','ftp_config_90_4.xml','ftp_config_90_5.xml','ftp_config_90_6.xml','ftp_config_90_veml.xml','ftp_config_923.xml','ftp_config_930.xml','ftp_config_930rk.xml','ftp_config_aut.xml','ftp_bbss.xml','ftp_bbssdb.xml','ftp_config_khbz_SCFirstPhoneInform.xml','ftp_config_khbz_SCFlowSettle.xml','ftp_config_khbz_SCVirtFlowSettle.xml','ftp_config_khnp.xml','ftp_config_mebp.xml','ftp_config_np.xml','ftp_config_qwcx.xml','ftp_config_rk5411.xml','ftp_config_tyzf_hb.xml','ftp_config_tyzf_payinfo.xml','ftp_config_tyzf_scsettle.xml','ftp_config_tyzf_tbsettle.xml','ftp_config_tyzf_upay.xml','ftp_rzxxtb_56.xml','job_EbossFtpReqRespAudit.xml','job_FtpFileSumTask.xml','job_FtpMonitorSumTask.xml','job_FtpSpaceSumTask.xml']

reCompile = re.compile(".*xml")
for file in fileList:
    if not reCompile .search(file):
        continue

    if file not in fileNameList:
        continue


    try:
        # 使用minidom解析器打开 XML 文档
        DOMTree = xml.dom.minidom.parse("C:\\Users\\Lenovo\\Desktop\\新建文件夹\\"+file)
        collection = DOMTree.documentElement
        taskDefine = collection .getElementsByTagName("FtpTaskDefine")[0] .getElementsByTagName("TaskThread")[0] .childNodes[0].data
        # print(taskDefine)
        Values = collection.getElementsByTagName("ExtParameter")

        fileTypeList = []
        for Value in Values:
            Key = Value.getElementsByTagName('Key')
            if Key[0].childNodes[0].data != 'FILE_TYPE':
                continue
            datas = Value.getElementsByTagName('Value')
            for data in datas:
                fileTypeList .append(data.childNodes[0].data)
        print(file, '\t', taskDefine, '\t', ',' .join(fileTypeList))
    except Exception as error:
        pass

