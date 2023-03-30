# -*- coding: utf-8 -*

import json
import os
from email.utils import formataddr
from functools import wraps
import dingtalk.api
import logging
import logging.handlers
from alibabacloud_dingtalk.yida_1_0.client import Client as dingtalkyida_1_0Client
from alibabacloud_tea_openapi import models as open_api_models
from alibabacloud_dingtalk.yida_1_0 import models as dingtalkyida__1__0_models
from alibabacloud_tea_util import models as util_models
from alibabacloud_tea_util.client import Client as UtilClient
import schedule
import time
import pymysql
import smtplib
from email.mime.text import MIMEText
import datetime
from retrying import retry

# -------------全局配置开始-----------------
# 全局配置
DEVELOPMENT = False
# 应用密钥
SYSTEM_TOKEN = 'xxxxxxx'
# 操作者USER_ID
USER_ID_OPERATOR = 'xxxxxxxx'

# 应用APPKEY
APPKEY = "xxxxxxxx"
# 应用APPSECRET
APPSECRET = "xxxxxxxxxxx"
APPAGENTID = "xxxxxxxx"

# 应用Type
APP_TYPE = 'xxxxxxxxxxx'
# 页面-出差申请
FORM_UUID_BUSINESS_TRIP = "xxxxxxxxxxxxxxx"
# 页面-出差变更
FORM_UUID_BUSINESS_TRIP_CHANGE = "xxxxxxxxxxxxxxx"
# 页面-员工信息
FORM_UUID_EMPLOYEE_INFO = "xxxxxxxxxxxxxxx"
# 页面-外勤申请
FORM_UUID_FIELD_APPLICATION = "xxxxxxxxxxxxxxx"

# -------------全局配置结束--------------------


# -------------日志配置开始-----------------
# 日志文件存放目录
log_dir = 'logdir'
# 日志文件的名字
log_filename = "app.log"
# 日志格式化输出
LOG_FORMAT = "%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s"
# 日期格式
DATE_FORMAT = "%Y-%m-%d %H:%M:%S %p"

log_dir = os.path.dirname(os.getcwd()) + '/' + log_dir
if not os.path.exists(log_dir):
    os.makedirs(log_dir)
log_filename = log_dir + '/' + log_filename

# 一个日志50M, 超过 app.log 指定的大小会自动滚动创建日志文件  app.log.1, app.log.2, app.log.3

fp = logging.handlers.TimedRotatingFileHandler(log_filename, when='D', interval=1, backupCount=30, encoding='utf-8')

# 再创建一个handler，用于输出到控制台
fs = logging.StreamHandler()

if DEVELOPMENT:
    logging.basicConfig(level=logging.DEBUG, format=LOG_FORMAT, datefmt=DATE_FORMAT, handlers=[fp, fs])
else:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT, handlers=[fp, fs])


# -------------日志配置结束-----------------

class DingtalkUtils:
    """
    钉钉及宜搭工具类
    """
    access_token = ""
    update_time = ""

    def __init__(self):
        pass

    @staticmethod
    def create_client() -> dingtalkyida_1_0Client:
        """
        使用 Token 初始化账号Client
        @return: Client
        @throws Exception
        """
        config = open_api_models.Config()
        config.protocol = 'https'
        config.region_id = 'central'
        return dingtalkyida_1_0Client(config)

    @staticmethod
    def get_dingtalk_access_token(app_key, app_secret):
        """
        获取内部应用的Token
        """
        now = time.time()
        if DingtalkUtils.update_time == "" or now - float(DingtalkUtils.update_time) > 3200 * 1.5 \
                or DingtalkUtils.access_token == "":
            req = dingtalk.api.OapiGettokenRequest("https://oapi.dingtalk.com/gettoken")
            req.appkey = app_key
            req.appsecret = app_secret
            try:
                resp = req.getResponse("")
                logging.info(resp)
                DingtalkUtils.update_time = now
                DingtalkUtils.access_token = resp['access_token']
            except Exception as e:
                logging.error(e)
                raise e
        return DingtalkUtils.access_token

    @staticmethod
    def create_or_update_yidaform(app_key, app_secret, system_token, form_uuid, user_id, app_type, search_condition,
                                  form_data_json):
        """
        向宜搭表单创建或更新数据
        :param app_key:
        :param app_secret:
        :param system_token:
        :param form_uuid:
        :param user_id:
        :param app_type:
        :param search_condition:
        :param form_data_json:
        :return:
        """
        client = DingtalkUtils.create_client()
        create_or_update_form_data_headers = dingtalkyida__1__0_models.CreateOrUpdateFormDataHeaders()
        create_or_update_form_data_headers.x_acs_dingtalk_access_token = DingtalkUtils.get_dingtalk_access_token(
            app_key, app_secret)

        create_or_update_form_data_request = dingtalkyida__1__0_models.CreateOrUpdateFormDataRequest(
            no_execute_expression=False,
            system_token=system_token,
            form_uuid=form_uuid,
            user_id=user_id,
            app_type=app_type,
            search_condition=search_condition,
            form_data_json=form_data_json
        )

        try:
            resp = client.create_or_update_form_data_with_options(create_or_update_form_data_request,
                                                                  create_or_update_form_data_headers,
                                                                  util_models.RuntimeOptions())
            logging.debug(resp)
            return resp
        except Exception as err:
            logging.error(err)
            raise err

    @staticmethod
    def search_yidaform(app_key, app_secret, app_type, system_token, page_number, page_size, form_uuid, user_id,
                        search_condition, modified_from_time_gmt, modified_to_time_gmt):
        """
        从宜搭表单查询数据
        https://open-dev.dingtalk.com/apiExplorer#/?devType=org&api=yida_1.0%23SearchFormDataSecondGeneration
        :param app_key:
        :param app_secret:
        :param app_type:
        :param system_token:
        :param page_number:
        :param page_size:
        :param form_uuid:
        :param user_id:
        :param search_condition:
        :param modified_from_time_gmt:
        :param modified_to_time_gmt:
        :return:
        """
        client = DingtalkUtils.create_client()
        search_form_data_second_generation_headers = dingtalkyida__1__0_models.SearchFormDataSecondGenerationHeaders()
        search_form_data_second_generation_headers.x_acs_dingtalk_access_token = \
            DingtalkUtils.get_dingtalk_access_token(app_key, app_secret)
        search_form_data_second_generation_request = dingtalkyida__1__0_models.SearchFormDataSecondGenerationRequest(
            page_number=page_number,
            form_uuid=form_uuid,
            app_type=app_type,
            modified_from_time_gmt=modified_from_time_gmt,
            modified_to_time_gmt=modified_to_time_gmt,
            search_condition=search_condition,
            order_config_json='{"gmt_modified":"+"}',
            system_token=system_token,
            page_size=page_size,
            user_id=user_id,
        )

        try:
            resp = client.search_form_data_second_generation_with_options(search_form_data_second_generation_request,
                                                                          search_form_data_second_generation_headers,
                                                                          util_models.RuntimeOptions()).body
            logging.debug(resp)
            return resp
        except Exception as err:
            logging.error(err)
            raise err

    @staticmethod
    def business_trip_approve_finish_to_dingtalk(app_key, app_secret, user_id, biz_type, from_date, to_date,
                                                 duration_unit, calculate_model, tag_name, approve_id,
                                                 jump_url):
        """
        通知钉钉出差审批已经完成
        https://open-dev.dingtalk.com/apiExplorer#/?devType=org&api=dingtalk.oapi.attendance.approve.finish
        :param app_key:
        :param app_secret:
        :param user_id:
        :param biz_type: 1：加班 2：出差、外出 3：请假
        :param from_date: 2019-08-15 or 2019-08-15 AM or 2019-08-15 12:43
        :param to_date: 2019-08-15 or 2019-08-15 AM or 2019-08-15 12:43
        :param duration_unit: day or halfDay or hour：biz_type，为1时仅支持hour。
        :param calculate_model: 0：按自然日计算 or 1：按工作日计算
        :param tag_name: 请假 or 出差 or 外出 or 加班
        :param approve_id:
        :param jump_url:
        :return:
        """
        req = dingtalk.api.OapiAttendanceApproveFinishRequest(
            "https://oapi.dingtalk.com/topapi/attendance/approve/finish")
        req.userid = user_id
        req.biz_type = biz_type
        req.from_time = from_date
        req.to_time = to_date
        req.duration_unit = duration_unit
        req.calculate_model = calculate_model
        req.tag_name = tag_name
        req.approve_id = approve_id
        req.jump_url = jump_url

        try:
            resp = req.getResponse(DingtalkUtils.get_dingtalk_access_token(app_key, app_secret))
            logging.debug(resp)
            return resp
        except Exception as err:
            logging.error(err)
            raise err

    @staticmethod
    def business_trip_cancel_finish_to_dingtalk(app_key, app_secret, user_id, approve_id):
        """
        通知钉钉出差审批已经完成
        :param app_key:
        :param app_secret:
        :param approve_id:
        :return:
        """

        # 取消原表单与OA同步的数据
        req = dingtalk.api.OapiAttendanceApproveCancelRequest(
            "https://oapi.dingtalk.com/topapi/attendance/approve/cancel")
        req.userid = user_id
        req.approve_id = approve_id
        try:
            resp = req.getResponse(DingtalkUtils.get_dingtalk_access_token(app_key, app_secret))
            logging.debug(resp)
            return resp
        except Exception as e:
            logging.error(e)
            raise e

    @staticmethod
    def get_employee_id_on_job(app_key, app_secret, offset, size):
        """
        获取钉钉中在职员工的所有ID信息
        :param app_key:
        :param app_secret:
        :param offset:
        :param size:
        :return:
        """
        req = dingtalk.api.OapiSmartworkHrmEmployeeQueryonjobRequest(
            "https://oapi.dingtalk.com/topapi/smartwork/hrm/employee/queryonjob")
        req.status_list = "2,3,5,-1"
        req.offset = offset
        req.size = size

        try:
            resp = req.getResponse(DingtalkUtils.get_dingtalk_access_token(app_key, app_secret))
            logging.debug(resp)
            return resp
        except Exception as e:
            logging.error(e)
            raise e

    @staticmethod
    def get_employee_info_by_id(app_key, app_secret, user_Id):
        req = dingtalk.api.OapiSmartworkHrmEmployeeV2ListRequest(
            "https://oapi.dingtalk.com/topapi/smartwork/hrm/employee/v2/list")
        req.agentid = APPAGENTID
        req.userid_list = user_Id
        try:
            resp = req.getResponse(DingtalkUtils.get_dingtalk_access_token(app_key, app_secret))
            logging.debug(resp)
            return resp
        except Exception as e:
            logging.error(e)
            raise e

    @staticmethod
    def get_department_list(app_key, app_secret, dept_id):
        req = dingtalk.api.OapiV2DepartmentListsubRequest("https://oapi.dingtalk.com/topapi/v2/department/listsub")
        req.dept_id = dept_id
        try:
            resp = req.getResponse(DingtalkUtils.get_dingtalk_access_token(app_key, app_secret))
            logging.debug(resp)
            return resp
        except Exception as err:
            logging.error(err)
            raise err

# -------------自定义开发开始--------------------


# -------------邮箱通知代码开始--------------------
def send_an_error_message(program_name, error_name, error_detail):
    """
    :param program_name: 运行的程序名称
    :param error_name: 错误名
    :param error_detail: 错误的详细信息
    :return: 程序出错时发送邮件提醒
    """
    # SMTP服务器配置
    SMTP_SERVER = "xxxxxxxxxx"
    EMAIL_ADDRESS = "xxxxxxxxxx"
    AUTHORIZATION_CODE = "xxxxxxxxxxx"

    # 发件人与收件人
    sender = EMAIL_ADDRESS
    receivers = "xxxxxxxxxx"

    # 获取程序出错的时间
    error_time = datetime.datetime.strftime(datetime.datetime.today(), "%Y-%m-%d %H:%M:%S:%f")
    # 邮件内容
    subject = "【宜搭异常提醒】{name}-{date}".format(name=program_name, date=error_time)  # 邮件的标题
    content = '''
    <div class="emailcontent" style="width:100%;max-width:720px;text-align:left;margin:0 auto;padding-top:80px;padding-bottom:20px">
        <div class="emailtitle">
            <h1 style="color:#fff;background:#51a0e3;line-height:70px;font-size:24px;font-weight:400;padding-left:40px;margin:0">程序运行异常通知</h1>
            <div class="emailtext" style="background:#fff;padding:20px 32px 20px">
                <p style="color:#6e6e6e;font-size:13px;line-height:24px">程序：<span style="color:red;">【{program_name}】</span>运行过程中出现异常错误，下面是具体的异常信息，请及时核查处理！</p>
                <table cellpadding="0" cellspacing="0" border="0" style="width:100%;border-top:1px solid #eee;border-left:1px solid #eee;color:#6e6e6e;font-size:16px;font-weight:normal">
                    <thead>
                        <tr>
                            <th colspan="2" style="padding:10px 0;border-right:1px solid #eee;border-bottom:1px solid #eee;text-align:center;background:#f8f8f8">程序异常详细信息</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td style="padding:10px 0;border-right:1px solid #eee;border-bottom:1px solid #eee;text-align:center;width:100px">异常简述</td>
                            <td style="padding:10px 20px 10px 30px;border-right:1px solid #eee;border-bottom:1px solid #eee;line-height:30px">{error_name}</td>
                        </tr>
                        <tr>
                            <td style="padding:10px 0;border-right:1px solid #eee;border-bottom:1px solid #eee;text-align:center">异常详情</td>
                            <td style="padding:10px 20px 10px 30px;border-right:1px solid #eee;border-bottom:1px solid #eee;line-height:30px">{error_detail}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>'''.format(program_name=program_name, error_name=error_name, error_detail=error_detail)  # 邮件的正文部分
    # 实例化一个文本对象
    massage = MIMEText(content, 'html', 'utf-8')
    massage['Subject'] = subject  # 标题
    massage['From'] = formataddr(("IT提醒", sender))
    massage['To'] = receivers  # 收件人

    try:
        mail = smtplib.SMTP_SSL(SMTP_SERVER, 994)  # 连接SMTP服务，默认465和944这里用994
        mail.login(EMAIL_ADDRESS, AUTHORIZATION_CODE)  # 登录到SMTP服务
        mail.sendmail(sender, receivers, massage.as_string())  # 发送邮件
        print("成功发送了一封邮件到" + receivers)
    except smtplib.SMTPException as ex:
        print("邮件发送失败！")


def email(func):
    @wraps(func)
    def wrapper(*args, **keyword):
        try:
            temp = func(*args, **keyword)
            return temp
        except Exception as e:
            send_an_error_message("宜搭", error_name="宜搭代码错误", error_detail=e.args)
            raise e
    return wrapper

# -------------邮箱通知代码结束--------------------


@retry(stop_max_attempt_number=3)
@email
def get_employee_on_job():
    # 获取在职员工的ID信息
    employee_list = []
    offset = 0

    while True:
        try:
            resp = DingtalkUtils.get_employee_id_on_job(APPKEY, APPSECRET, offset, 50)
            employee_list.extend(resp["result"]["data_list"])
            if "next_cursor" not in resp["result"]:
                break
            else:
                offset = resp["result"]["next_cursor"]
        except Exception as err:
            time.sleep(2)
            raise err

    # 通过在职员工ID获取在职员工信息
    dict1 = {}
    for i, userid in enumerate(employee_list):
        userid_list = userid
        try:
            resp = DingtalkUtils.get_employee_info_by_id(APPKEY, APPSECRET, userid_list)
            dict1[userid] = resp["result"][0]
        except Exception as err:
            time.sleep(2)
            raise err
    return dict1


@retry(stop_max_attempt_number=3)
@email
def get_depart_parent_all():
    logging.info("获取父部门列表")
    depart_list = {}
    dept_id = 1
    try:
        resp = DingtalkUtils.get_department_list(APPKEY, APPSECRET, dept_id)

        for dep1 in resp["result"]:
            # 1级部门
            depart_list[dep1["dept_id"]] = {}
            depart_list[dep1["dept_id"]]["name"] = dep1["name"]
            parent = dep1["name"]
            depart_list[dep1["dept_id"]]["first_level"] = parent
            dept_id = dep1["dept_id"]
            resp = DingtalkUtils.get_department_list(APPKEY, APPSECRET, dept_id)

            for dep2 in resp["result"]:
                # 2级部门
                depart_list[dep2["dept_id"]] = {}
                depart_list[dep2["dept_id"]]["name"] = dep2["name"]
                depart_list[dep2["dept_id"]]["first_level"] = parent
                dept_id = dep2["dept_id"]
                resp = DingtalkUtils.get_department_list(APPKEY, APPSECRET, dept_id)

                for dep3 in resp["result"]:
                    # 3级部门
                    depart_list[dep3["dept_id"]] = {}
                    depart_list[dep3["dept_id"]]["name"] = dep3["name"]
                    depart_list[dep3["dept_id"]]["first_level"] = parent
                    dept_id = dep3["dept_id"]
                    resp = DingtalkUtils.get_department_list(APPKEY, APPSECRET, dept_id)

                    for dep4 in resp["result"]:
                        # 4级部门
                        depart_list[dep4["dept_id"]] = {}
                        depart_list[dep4["dept_id"]]["name"] = dep3["name"]
                        depart_list[dep4["dept_id"]]["first_level"] = parent
        return depart_list
    except Exception as e:
        logging.error(e)
        time.sleep(2)
        raise e


@retry(stop_max_attempt_number=3)
@email
def business_trip_to_dingtalk(from_time="", to_time=""):
    logging.info("----开始出差同步----")
    modified_from_time_gmt = from_time
    modified_to_time_gmt = to_time
    count = 0
    logging.debug("抓取时间间隔：" + modified_from_time_gmt + "&" + modified_to_time_gmt)

    # 获取宜搭出差表单数据
    search_condition = '[{"key": "processApprovedResult", "value": ["agree"], "type": "ARRAY", ' \
                       '"operator": "in",  "componentName": "SelectField" }]'
    try:
        resp = DingtalkUtils.search_yidaform(APPKEY, APPSECRET, APP_TYPE, SYSTEM_TOKEN, 1, 100, FORM_UUID_BUSINESS_TRIP,
                                             USER_ID_OPERATOR, search_condition, modified_from_time_gmt,
                                             modified_to_time_gmt)
    except Exception as err:
        time.sleep(2)
        logging.error("----员工出差数据更新出错，已更新" + str(count) + "条")
        raise err

    # 同步宜搭出差表单数据至钉钉
    for data in resp.data:
        business_trip_data = data.form_data
        creator_id = data.creator_user_id
        all_travel_time = business_trip_data["tableField_l63i9o7v"]
        creator = business_trip_data["textField_l6un702e"]  # 申请人
        for travel_time in all_travel_time:
            from_date = time.strftime("%Y-%m-%d", time.localtime(int(travel_time["dateField_l6vzymfp"] / 1000)))
            form_date_slot = "AM" if travel_time["selectField_l6vzymfr_id"] == "上午" else "PM"
            from_date = from_date + " " + form_date_slot
            to_date = time.strftime("%Y-%m-%d", time.localtime(int(travel_time["dateField_l6vzymfq"] / 1000)))
            to_date_slot = "AM" if travel_time["selectField_l6vzymfs_id"] == "上午" else "PM"
            to_date = to_date + " " + to_date_slot
            instance_id = data.serial_number
            jump_url = "https://s1k3ix.aliwork.com/" + APP_TYPE + "/processDetail?formInstId=" \
                       + data.form_instance_id
            try:
                resp = DingtalkUtils.business_trip_approve_finish_to_dingtalk(APPKEY, APPSECRET, creator_id, 2,
                                                                              from_date, to_date, "halfDay", 1,
                                                                              "出差", instance_id, jump_url)
                if resp["errcode"] == 0:
                    logging.info("----出差数据更新成功：" + creator + " & " + creator_id + " & "
                                 + str(from_date) + " & " + str(to_date) + " & " + instance_id)
                count = count + 1
            except Exception as err:
                logging.error("----员工出差数据更新出错，已更新" + str(count) + "条")
                time.sleep(2)
                raise err

    logging.info("----员工出差数据更新完成，共" + str(count) + "条")


@retry(stop_max_attempt_number=3)
@email
def filed_application_to_dingtalk(from_time="", to_time=""):
    logging.info("----开始外勤同步----")

    # 查询或变更的时间范围（该范围内进行同步）
    now = datetime.datetime.now()
    minute = now.strftime("%M")
    hour = now.strftime("%H")
    hour_str = now.strftime("%Y-%m-%d ")
    if int(minute) >= 30:
        modified_from_time_gmt = hour_str + hour + ":00:00"
        modified_to_time_gmt = hour_str + hour + ":29:59"
    else:
        hour = str(int(hour) - 1)
        modified_from_time_gmt = hour_str + hour + ":30:00"
        modified_to_time_gmt = hour_str + hour + ":59:59"

    # 根据输入的时间做判断
    if from_time and to_time:
        modified_from_time_gmt = from_time
        modified_to_time_gmt = to_time

    logging.debug("抓取时间间隔：" + modified_from_time_gmt + "&" + modified_to_time_gmt)

    count = 0

    # 获取宜搭出差表单数据
    search_condition = '[{"key": "processApprovedResult", "value": ["agree"], "type": "ARRAY", ' \
                       '"operator": "in",  "componentName": "SelectField" }]'
    try:
        resp = DingtalkUtils.search_yidaform(APPKEY, APPSECRET, APP_TYPE, SYSTEM_TOKEN, 1, 100,
                                             FORM_UUID_FIELD_APPLICATION, USER_ID_OPERATOR, search_condition,
                                             modified_from_time_gmt, modified_to_time_gmt)
    except Exception as err:
        time.sleep(2)
        logging.error("----外勤同步数据更新出错，已更新" + str(count) + "条")
        raise err

    # 同步宜搭出差表单数据至钉钉
    for data in resp.data:
        instance_id = data.serial_number
        form_instance_id = data.form_instance_id
        creator_id = data.creator_user_id
        data = data.form_data
        field_datas = data["tableField_l63i9o7v"]
        creator = data["textField_l6un702e"]  # 申请人
        for filed_data in field_datas:
            from_date = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(filed_data["dateField_l9c53yak"] / 1000)))
            to_date = time.strftime("%Y-%m-%d %H:%M", time.localtime(int(filed_data["dateField_l9c53yam"] / 1000)))

            jump_url = "https://s1k3ix.aliwork.com/" + APP_TYPE + "/processDetail?formInstId=" \
                       + form_instance_id
            try:
                resp = DingtalkUtils.business_trip_approve_finish_to_dingtalk(APPKEY, APPSECRET, creator_id, 2,
                                                                              from_date, to_date, "hour", 1,
                                                                              "外出", instance_id, jump_url)
                if resp["errcode"] == 0:
                    logging.info("----外勤同步更新成功：" + creator + " & " + creator_id + " & "
                                 + str(from_date) + " & " + str(to_date) + " & " + instance_id)
                count = count + 1
            except Exception as err:
                logging.error("----外勤同步更新出错，已更新" + str(count) + "条")
                time.sleep(2)
                raise err

    logging.info("----外勤同步更新完成，共" + str(count) + "条")


@retry(stop_max_attempt_number=3)
@email
def business_trip_change_to_dingtalk(from_time="", to_time=""):
    logging.info("----开始出差变更同步----")

    count = 0

    # 查询或变更的时间范围（该范围内进行同步）
    now = datetime.datetime.now()
    minute = now.strftime("%M")
    hour = now.strftime("%H")
    hour_str = now.strftime("%Y-%m-%d ")
    if int(minute) >= 30:
        modified_from_time_gmt = hour_str + hour + ":00:00"
        modified_to_time_gmt = hour_str + hour + ":29:59"
    else:
        hour = str(int(hour) - 1)
        modified_from_time_gmt = hour_str + hour + ":30:00"
        modified_to_time_gmt = hour_str + hour + ":59:59"
    # 根据输入的时间做判断
    if from_time and to_time:
        modified_from_time_gmt = from_time
        modified_to_time_gmt = to_time
    logging.debug("抓取时间间隔：" + modified_from_time_gmt + "&" + modified_to_time_gmt)

    # 先执行出差同步，保证数据一致
    business_trip_to_dingtalk(modified_from_time_gmt, modified_to_time_gmt)

    # 获取宜搭表单出差变更中的数据集合, 数据中的pagesize=100后面数据量大了以后需要更新写
    search_condition = '[{"key": "processApprovedResult", "value": ["agree"], "type": "ARRAY", ' \
                       '"operator": "in", "componentName": "SelectField"}]'
    try:
        resp = DingtalkUtils.search_yidaform(APPKEY, APPSECRET, APP_TYPE, SYSTEM_TOKEN, 1, 100,
                                             FORM_UUID_BUSINESS_TRIP_CHANGE, USER_ID_OPERATOR, search_condition,
                                             modified_from_time_gmt, modified_to_time_gmt)
    except Exception as err:
        time.sleep(2)
        raise err

    # 删除钉钉中出差变更前的数据,并同步最新数据
    for data in resp.data:
        # 判断出差变更类型
        is_cancel = False
        business_trip_data = data.form_data  # 出差数据
        type = business_trip_data["selectField_l78pm1uc"]
        logging.debug(type)
        creator_id = data.creator_user_id  # 员工UserID
        pre_apply_id = ""
        creator = business_trip_data["textField_l6un702e"]  # 申请人

        if type == "取消未变更过的出差":
            is_cancel = True
            pre_apply_id = json.loads(json.loads(business_trip_data["associationFormField_l7r2pt5q_id"]))[0]["title"]  # 原出差申请单ID
        if type == "取消已变更过的出差":
            is_cancel = True
            pre_apply_id = json.loads(json.loads(business_trip_data["associationFormField_l7r2pt5r_id"]))[0]["title"]  # 原出差申请单ID
        if type == "首次变更":
            is_cancel = False
            pre_apply_id = json.loads(json.loads(business_trip_data["associationFormField_l6xjqm71_id"]))[0]["title"]  # 原出差申请单ID
        if type == "非首次变更":
            is_cancel = False
            pre_apply_id = json.loads(json.loads(business_trip_data["associationFormField_l78pm1tz_id"]))[0]["title"]  # 原出差申请单ID

        # 删除钉钉中出差变更的考勤数据
        try:
            resp = DingtalkUtils.business_trip_cancel_finish_to_dingtalk(APPKEY, APPSECRET, creator_id, pre_apply_id)
            if resp["errcode"] == 0:
                logging.info("----原关联单取消成功成功：" + creator + " & " + creator_id + " & " + pre_apply_id)
        except Exception as e:
            if e.errcode == 400002:
                logging.error(
                    "数据已被删除，原出差数据删除失败：" + str(creator) + " & " + str(creator_id) + " & " + str(pre_apply_id))
            else:
                time.sleep(2)
                logging.error("原出差数据删除失败：" + str(creator) + " & " + str(creator_id) + " & " + str(pre_apply_id))
                raise e

        if not is_cancel:
            # 将宜搭变更中最新数据同步至钉钉考勤
            all_trip_time = business_trip_data["tableField_l63i9o7v"]  # 所有出差时间
            for time_data in all_trip_time:
                from_date = time.strftime("%Y-%m-%d", time.localtime(int(time_data["dateField_l6vzymfp"] / 1000)))
                form_date_slot = "AM" if time_data["selectField_l6vzymfr_id"] == "上午" else "PM"
                from_date = from_date + " " + form_date_slot
                to_date = time.strftime("%Y-%m-%d", time.localtime(int(time_data["dateField_l6vzymfq"] / 1000)))
                to_date_slot = "AM" if time_data["selectField_l6vzymfs_id"] == "上午" else "PM"
                to_date = to_date + " " + to_date_slot
                instance_id = data.serial_number
                jump_url = "https://s1k3ix.aliwork.com/" + APP_TYPE + "/processDetail?formInstId=" \
                           + data.form_instance_id
                try:
                    resp = DingtalkUtils.business_trip_approve_finish_to_dingtalk(APPKEY, APPSECRET, creator_id, 2,
                                                                                  from_date, to_date, "halfDay", 1,
                                                                                  "出差", instance_id, jump_url)
                    if resp["errcode"] == 0:
                        logging.info("----出差数据变更更新成功：" + creator + " & " + creator_id + " & "
                                     + str(from_date) + " & " + str(to_date) + " & " + instance_id)
                        count = count + 1
                except Exception as err:
                    time.sleep(2)
                    logging.error("----员工出差变更数据更新出错，已更新" + str(count) + "条")
                    raise err
    logging.info("----员工出差变更数据更新完成，共" + str(count) + "条")


@retry(stop_max_attempt_number=3)
@email
def update_form_employee_info():
    logging.info("----开始更新员工信息表----")

    # 获取部门ID:一级部门字典
    depart_parent = get_depart_parent_all()

    # 获取员工信息字典
    employee_list = get_employee_on_job()

    # 将员工信息插入到宜搭员工信息表中
    for employee in employee_list.keys():
        country = ""  # 国家：textField_l6w1tckk
        name = ""  # 姓名：textField_l6c046bz
        user_id = ""  # 用户ID：textField_l6d77ijn
        first_department = ""  # 一级部门：textField_l6c046c0
        employee_id = ""  # 员工编号：textField_l6oi3r6n
        workplace = ""  # 工作地点：textField_l6oq5scz
        contract_company = ""  # 合同公司：textField_l6vjo0rx
        bank_name = ""  # 开户行：textField_l6vjo0ry
        bank_no = ""  # 银行卡后四位：textField_l6vjo0rz
        title = ""  # 职位：textField_l6w1tckl
        onboarding_time = ""  # 入职时间：textField_l6w1tckm
        department_id = []

        user_id = employee_list[employee]["userid"]
        employee_data = employee_list[employee]["field_data_list"]

        for data in employee_data:
            if data["field_name"] == "姓名":
                name = data["field_value_list"][0].get("value", "")
            if data["field_name"] == "工号":
                employee_id = data["field_value_list"][0].get("value", "")
            if data["field_name"] == "办公地点":
                workplace = data["field_value_list"][0].get("value", "")
            if data["field_name"] == "合同公司":
                contract_company = data["field_value_list"][0].get("value", "")
            if data["field_name"] == "开户行":
                bank_name = data["field_value_list"][0].get("value", "")
            if data["field_name"] == "银行卡号后四位":
                bank_no = data["field_value_list"][0].get("value", "")
            if data["field_name"] == "国家":
                country = data["field_value_list"][0].get("value", "")
            if data["field_name"] == "部门id":
                department_id = data["field_value_list"][0].get("value", "").split("|")
            if data["field_name"] == "职位":
                title = data["field_value_list"][0].get("value", "")
            if data["field_name"] == "入职时间":
                onboarding_time = data["field_value_list"][0].get("value", "")

        pre_depart = ""

        for dep_data in department_id:
            dep_id = int(dep_data)
            if depart_parent[dep_id]["first_level"] == pre_depart:
                continue
            first_department = depart_parent[dep_id]["first_level"]
            if first_department == "待定用户":
                first_department = ""
            search_condition = '[{ "key": "textField_l6d77ijn", "value": "' \
                               + employee_list[employee]["userid"] \
                               + '", "type": "TEXT",  "operator": "eq",  "componentName": "TextField" }, ' \
                                 '{ "key": "textField_l6c046c0",  "value": "' \
                               + first_department \
                               + '",  "type": "TEXT",  "operator": "eq",  "componentName": "TextField" }]'
            form_data_json = '{"textField_l6c046bz":"' + name.strip() + \
                             '","textField_l6d77ijn":"' + user_id.strip() + \
                             '","textField_l6c046c0":"' + first_department.strip() + \
                             '","textField_l6oq5scz":"' + workplace.strip() + \
                             '","textField_l6vjo0rx":"' + contract_company.strip() + \
                             '","textField_l6vjo0ry":"' + bank_name.strip() + \
                             '","textField_l6vjo0rz":"' + bank_no.strip() + \
                             '","textField_l6w1tckk":"' + country.strip() + \
                             '","textField_l6oi3r6n":"' + employee_id.strip() + \
                             '","textField_l6w1tckl":"' + title.strip() + \
                             '","textField_l6w1tckm":"' + onboarding_time.strip() + '"}'
            try:
                resp = DingtalkUtils.create_or_update_yidaform(APPKEY, APPSECRET, SYSTEM_TOKEN, FORM_UUID_EMPLOYEE_INFO,
                                                               USER_ID_OPERATOR, APP_TYPE, search_condition,
                                                               form_data_json)
                pre_depart = depart_parent[dep_id]["first_level"]

            except Exception as err:
                time.sleep(2)
                logging.error(err)
                raise err
    logging.info("----结束更新员工信息表----")


def inset_global_city_form() -> None:
    client = DingtalkUtils.create_client()
    save_form_data_headers = dingtalkyida__1__0_models.SaveFormDataHeaders()
    save_form_data_headers.x_acs_dingtalk_access_token = DingtalkUtils.token

    # 链接服务端
    conn_obj = pymysql.connect(
        host='10.81.1.16',  # MySQL服务端的IP地址
        port=3306,  # MySQL默认PORT地址(端口号)
        user='root',  # 用户名
        password='5!bY&P9e',  # 密码,也可以简写为passwd
        database='travel_area',  # 库名称,也可以简写为db
        charset='utf8'  # 字符编码
    )

    cursor = conn_obj.cursor()
    sql = """
        select * from travel_area;
        """
    cursor.execute(sql)
    res = cursor.fetchall()

    for data in res:
        if data[3] == 4:
            index = data[2].split(',')
            continent = res[int(index[1]) - 1][4]
            country = res[int(index[2]) - 1][4]
            province = res[int(index[3]) - 1][4]
            city = data[4]

            save_form_data_request = dingtalkyida__1__0_models.SaveFormDataRequest(
                system_token='8F966HB1KPI267FO9BNZF73A9DYP3PK2YZ26LBB',
                form_uuid='FORM-78766VC1HJX2UMP29JZP0CBQ3W5P3N6DVAU6LR1',
                user_id='020768514835588593',
                app_type='APP_T3NXE1MYZ6O34NXJL12N',

                form_data_json='{"textField_l6ugmdmc":"' + continent +
                               '",' + '"textField_l6ugmdmd":"' + country +
                               '",' + '"textField_l6ugmdme":"' + province +
                               '",' + '"textField_l6ugmdmf":"' + city + '"}'
            )
            print(data)
            try:
                client.save_form_data_with_options(save_form_data_request, save_form_data_headers,
                                                   util_models.RuntimeOptions())
            except Exception as err:
                logging.error(err)
                if not UtilClient.empty(err.code) and not UtilClient.empty(err.message):
                    # err 中含有 code 和 message 属性，可帮助开发定位问题
                    pass

    cursor.close()
    conn_obj.close()

    logging.info("---全球地址级联表（四级）写入完成")


# -------------定时任务配置开始--------------------
schedule.every().wednesday.at("05:00").do(update_form_employee_info)
schedule.every().hour.at("10:00").do(business_trip_change_to_dingtalk)
schedule.every().hour.at("40:00").do(business_trip_change_to_dingtalk)
schedule.every().hour.at("10:00").do(filed_application_to_dingtalk)
schedule.every().hour.at("40:00").do(filed_application_to_dingtalk)

# -------------定时任务配置结束--------------------


if __name__ == '__main__':
    # business_trip_change_to_dingtalk("2023-03-11 10:00:00", "2023-03-16 16:59:59")
    # filed_application_to_dingtalk("2023-03-11 10:00:00", "2023-03-16 16:59:59")

    while True:
        schedule.run_pending()
        time.sleep(1)
