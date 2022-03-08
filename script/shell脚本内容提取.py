# Time     2022/03/01 09::09
# Auhtor   
# ide      PyCharm
# Verion   1.0
# function 默认
import os
import re

filePath = 'C:\\Users\\Lenovo\\Desktop\\新建文件夹'
reCompile = re .compile('.*properties')
strCompile = re .compile('log4j.appender.dailyfile.File=(\S+)')

taskList = ['ftp_10.properties','ftp_5_56.properties','ftp_6_56.properties','ftp_conf_vces_6711.properties','ftp_0.properties','ftp_10.properties','ftp_10_kh.properties','ftp_100.properties','ftp_config_1001.properties','ftp_config_10013.properties','ftp_config_1003.properties','ftp_config_101.properties','ftp_config_1022.properties','ftp_config_1160.properties','ftp_config_1161.properties','ftp_config_1162.properties','ftp_config_1163.properties','ftp_config_1167.properties','ftp_config_1168.properties','ftp_180.properties','ftp_200_201.properties','ftp_21_301.properties','ftp_config_333.properties','ftp_config_3999.properties','ftp_config_4000.properties','ftp_41.properties','ftp_config_4204.properties','ftp_config_436.properties','ftp_config_44.properties','ftp_config_44_2.properties','ftp_5015.properties','ftp_51.properties','ftp_541.properties','ftp_config_5411.properties','ftp_542.properties','ftp_config_58.properties','ftp_config_66_56.properties','ftp_8.properties','ftp_config_81801.properties','ftp_config_81802.properties','ftp_config_81803.properties','ftp_9.properties','ftp_90.properties','ftp_90_1.properties','ftp_90_2.properties','ftp_90_3.properties','ftp_90_4.properties','ftp_config_90_5.properties','ftp_config_90_6.properties','ftp_config_90_veml.properties','ftp_config_923.properties','ftp_930.properties','ftp_930rk.properties','ftp_config_aut.properties','ftp_bbss.properties','ftp_bbssdb.properties','ftp_config_khbz_SCFirstPhoneInform.properties','ftp_config_khbz_SCFlowSettle.properties','ftp_config_khbz_SCVirtFlowSettle.properties','ftp_config_khnp.properties','ftp_config_mebp.properties','ftp_config_np.properties','ftp_config_qwcx.properties','ftp_rk5411.properties','ftp_config_tyzf_hb.properties','ftp_config_tyzf_payinfo.properties','ftp_config_tyzf_scsettle.properties','ftp_config_tyzf_tbsettle.properties','ftp_config_tyzf_upay.properties','ftp_rzxxtb_56.properties','job_EbossFtpReqRespAudit.properties','job_FtpFileSumTask.properties','job_FtpMonitorSumTask.properties','job_FtpSpaceSumTask.properties']

fileNameList = []
for path, folder, files in os .walk(filePath):
    fileNameList = files

for file in fileNameList:
    if not reCompile .search(file):
        continue

    if file not in taskList:
        continue

    newFilePath = os .path .join(filePath,file)

    fileDesc = open(newFilePath)

    fileDescStr = fileDesc .read()

    str = strCompile .findall(fileDescStr)
    # print(fileDescStr)
    # print(str)

    # print(file, '\t', ',' .join(str))
    print(file, '\t', str[0].split('/')[-1])
    fileDesc .close()
