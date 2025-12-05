# apps/indicator/data_fetcher.py

import re
import json
import socket
import logging
from datetime import date, datetime
import xml.etree.ElementTree as ET
from apps.indicator.secapi_invoker import SecAPIInvoker
from common.utils.utils import SQLBotLogUtil


# **************************************************************************
# 常量配置
# ************************************************************************** #

# 密服 JAR 包在容器中的路径
SEC_JAR_PATH = "/home/sa/sa/jar/csc-secapi-1.0.8.3-jdk-1.6.jar"

# 定义ESB客户端的 IP 地址和端口
SERVER_IP = "9.6.232.51"
SERVER_PORT = 15301

# 项目编号
SOURCE_NODE = "121100"  # 模型平台项目编号
DEST_NODE = "106100"    # ESB服务项目编号


# ************************************************************************** #
# 方法实现
# ************************************************************************** #

def parse_index_data(xml_data):
    xml_string = xml_data[42:]

    # 解析 XML
    root = ET.fromstring(xml_string)
    
    # 找到 Body 元素
    body_element = root.find('Body')

    # 转换为字典
    body_dict = {}
    for child in body_element:
        body_dict[child.tag] = child.text

    # 包装为 JSON 结构
    # result = {"Body": body_dict}

    # 输出 JSON
    json_output = json.dumps(body_dict, indent=2, ensure_ascii=False)
    SQLBotLogUtil.info("解析后的指标数据：")
    SQLBotLogUtil.info(json_output)
    return json_output


def send_xml_over_tcp(mac, xml_message, source_node):

    # 创建一个 TCP socket 对象
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        # 连接到服务器
        client_socket.connect((SERVER_IP, SERVER_PORT))
        SQLBotLogUtil.info(f"成功连接到服务器 {SERVER_IP}:{SERVER_PORT}")


        # 定义 XML 格式的报文
        xml_len = len(xml_message.encode('utf-8')) + 34
        mac_len = '0030'
        mac = mac.decode('utf-8')
        SQLBotLogUtil.debug(f"mac_len 类型：{type(mac_len)}")
        SQLBotLogUtil.debug(f"mac 类型：{type(mac)}")
        SQLBotLogUtil.debug(f"source_node 类型：{type(source_node)}")
        SQLBotLogUtil.debug(f"xml_message 类型：{type(xml_message)}")

        send_message = str(xml_len).zfill(8) + mac_len + mac + source_node + xml_message
        # 发送 XML 报文
        try:
            client_socket.sendall(send_message.encode('utf-8'))
        except Exception as e:
            SQLBotLogUtil.error('调用 ESB 发送报文出错' + '*'*30)
            SQLBotLogUtil.error(str(e))
            raise e
        SQLBotLogUtil.info("已发送 XML 报文:")
        SQLBotLogUtil.info(send_message)

        # 接收服务器的响应
        response = b''
        while True:
            SQLBotLogUtil.debug('接收数据中... ...')
            data = client_socket.recv(4096)
            if not data:
                break
            response += data

        SQLBotLogUtil.info("\n收到服务器响应:")
        SQLBotLogUtil.info(response.decode('utf-8'))

        return response.decode('utf-8')

    except ConnectionRefusedError:
        SQLBotLogUtil.error(f"无法连接到服务器 {SERVER_IP}:{SERVER_PORT}")
        raise ConnectionRefusedError
    except socket.timeout:
        SQLBotLogUtil.error("连接超时，请检查网络或服务器状态")
        raise socket.timeout
    except Exception as e:
        SQLBotLogUtil.error(f"发生错误: {e}")
        raise e
    finally:
        # 关闭 socket 连接
        client_socket.close()
        SQLBotLogUtil.info("\n连接已关闭")

def generate_mac(source_node, dest_node, xml_message):
    # 使用上下文管理器确保资源正确释放
    with SecAPIInvoker(SEC_JAR_PATH) as secapi:
        # 1. 节点初始化
        success, error_msg = secapi.node_init(source_node)

        if success:
            SQLBotLogUtil.info("节点初始化成功")
        else:
            SQLBotLogUtil.error(f"节点初始化失败: {error_msg}")
            # 如果初始化失败，后续操作可能无法进行
            return

        # 2. 生成MAC
        success, mac_result, error_msg = secapi.generate_mac(source_node, dest_node, xml_message)

        if success and mac_result is not None:
            #print(f"生成的MAC: {mac_result.hex()}")
            SQLBotLogUtil.info(f"生成mac成功")
            return mac_result
        else:
            SQLBotLogUtil.error(f"生成MAC失败: {error_msg}")

def get_index_data(request_data):
    
    SQLBotLogUtil.debug('获取指标数据：' + '*'*30)

    xml_message = """<?xml version="1.0" encoding="UTF-8"?>""" + \
                    """<Service>""" + \
                        """<SysHead>""" + \
                            """<SvcCd>9203020801</SvcCd>""" + \
                            """<SvcScn>01</SvcScn>""" + \
                            """<SvcVrsn></SvcVrsn>""" + \
                            """<SvcSplrTxnCd></SvcSplrTxnCd>""" + \
                            """<Vrsn></Vrsn>""" + \
                            """<BIZRT_SRVRM_CENT_IDN>1</BIZRT_SRVRM_CENT_IDN>""" + \
                            """<BnkCd>001</BnkCd>""" + \
                            """<CnsmrSysId>121100</CnsmrSysId>""" + \
                            """<TxnDt>""" + str(date.today()) + """</TxnDt>""" + \
                            """<TxnTm>""" + datetime.now().strftime("%H%M%S") + """</TxnTm>""" + \
                            """<AcgDt></AcgDt>""" + \
                            """<CnsmrSeqNo>""" + datetime.now().strftime('%Y%m%d%H%M%S') + """</CnsmrSeqNo>""" + \
                            """<TxnChnlTp></TxnChnlTp><ChnlDtl></ChnlDtl><TxnTmlId></TxnTmlId>""" + \
                            """<CnsmrSvrId>""" + SERVER_IP + """</CnsmrSvrId>""" + \
                            """<OrigCnsmrSeqNo>""" + datetime.now().strftime('%Y%m%d%H%M%S') + """</OrigCnsmrSeqNo>""" + \
                            """<ORGN_GLBL_BIZ_SWFNO></ORGN_GLBL_BIZ_SWFNO>""" + \
                            """<OrigCnsmrId>""" + SOURCE_NODE + """</OrigCnsmrId>""" + \
                            """<OrigTmlId></OrigTmlId>""" + \
                            """<OrigCnsmrSvrId>""" + SERVER_IP + """</OrigCnsmrSvrId>""" + \
                            """<UsrLng></UsrLng><FileFlg></FileFlg><TERM_MACAD></TERM_MACAD>""" + \
                            """<SPLCT_TERM_IPADR></SPLCT_TERM_IPADR>""" + \
                        """</SysHead>""" + \
                        """<AppHead>""" + \
                            """<TxnTlrId>GZ88</TxnTlrId>""" + \
                            """<OrgId>27040101</OrgId>""" + \
                            """<TlrPwsd></TlrPwsd>""" + \
                            """<TlrLvl></TlrLvl><TlrTp></TlrTp><AprvFlg></AprvFlg>""" + \
                            """<AhrTlrInf type="array">""" + \
                                """<Struct>""" + \
                                    """<AhrTlrId></AhrTlrId><AhrOrgId></AhrOrgId><AhrTlrPswd></AhrTlrPswd><AhrTlrLvl></AhrTlrLvl><AhrTlrTp></AhrTlrTp>""" + \
                                """</Struct>""" + \
                            """</AhrTlrInf>""" + \
                            """<AprvTlrInf type="array">""" + \
                                """<Struct>""" + \
                                    """<AprvTlrId></AprvTlrId><AprvOrgId></AprvOrgId><AprvTlrLvl></AprvTlrLvl><AprvTlrTp></AprvTlrTp>""" + \
                                """</Struct>""" + \
                            """</AprvTlrInf>""" + \
                            """<AhrFlg></AhrFlg>""" + \
                            """<ORD_IFARR type="array">""" + \
                                """<Struct>""" + \
                                    """<SERV_ORD_NBR>0101010101010101</SERV_ORD_NBR>""" + \
                                    """<BIZ_ORD_NBR>0101010101010101</BIZ_ORD_NBR>""" + \
                                    """<ORGSR_ORD_NBR>0101010101010101</ORGSR_ORD_NBR>""" + \
                                    """<ORGBZ_ORD_NBR>0101010101010101</ORGBZ_ORD_NBR>""" + \
                                    """<TRD_ORD_NBR>0101010101010101</TRD_ORD_NBR>""" + \
                                    """<OGTRD_ORD_NBR>0101010101010101</OGTRD_ORD_NBR>""" + \
                                """</Struct>""" + \
                            """</ORD_IFARR>""" + \
                        """</AppHead>""" + \
                        """<RouterHead>""" + \
                            """<GLBL_ROUTE_TYP_IDCD></GLBL_ROUTE_TYP_IDCD><GLBL_ROUTE_IDCD></GLBL_ROUTE_IDCD>""" + \
                            """<CALC_UNEM_IDCD></CALC_UNEM_IDCD><TGT_UNEM_IDCD></TGT_UNEM_IDCD>""" + \
                            """<DB_VISIT_MODE_CD></DB_VISIT_MODE_CD><CSTNO_BRST_ECD></CSTNO_BRST_ECD>""" + \
                        """</RouterHead>""" + \
                        """<Body>""" + \
                            """<CURNT_PGQT>1</CURNT_PGQT>""" + \
                            """<CURNT_PG_KPRCD_QTY>20</CURNT_PG_KPRCD_QTY>""" + \
                            """<QRY_CNDT_JSSTR>""" + request_data + """</QRY_CNDT_JSSTR>""" + \
                        """</Body>""" + \
                    """</Service>"""

    mac = generate_mac(SOURCE_NODE, DEST_NODE, xml_message)

    try:
        index_data = send_xml_over_tcp(mac, xml_message, SOURCE_NODE)
        json_output = parse_index_data(index_data)
    except Exception as e:
        raise e

    return json_output

def call_data_api(sql: str):
    """
    Call data API with SQL and parameters to fetch data
    """
    
    # formated_param = format_api_params(sql)
    formated_param = extract_json_from_markdown(sql)
    
    try:
        json_output = get_index_data(formated_param)
        formated_data = format_api_response(json_output)
    except Exception as e:
        SQLBotLogUtil.error(str(e))

    return formated_data

def format_api_params(params: str) -> str:
    """
    Format parameters for API call
    """
    # 如果输入是markdown格式的SQL代码块，提取其中的SQL内容
    if params.startswith("```json") and params.endswith("```"):
        sql_content = params[7:-3]
    else:
        # 如果不是markdown格式，直接使用原始内容
        sql_content = params

    # 将json中的 \" 转换为 \\\"
    formatted_sql = sql_content.replace('\\"', '\\\\"')

    # 移除 \n 换行符
    formatted_sql = formatted_sql.replace('\n', '')

    return formatted_sql

def extract_json_from_markdown(md_text: str) -> str:
    """
    从 Markdown 中提取 JSON 代码块，并转为转义后的字符串
    输出格式: {\"key\": \"value\"}，适用于 JSON 嵌套场景
    """

    json_str = ""

    match = re.search(r"```(?:json|JSON)?\s*\n(.*?)\n```", md_text, re.DOTALL)
    if not match:
        raise ValueError("No JSON code block found in markdown")
    
    json_text = match.group(1).strip()
    data = json.loads(json_text)
    try:
        json_str = json.dumps(data, ensure_ascii=False, separators=(',', ':'))
    except Exception as e:
        SQLBotLogUtil.error(str(e))
    SQLBotLogUtil.info(f'格式转换后的json字符串如下：{json_str}')
    return json_str.replace('"', '\\\"')

# 示例
if __name__ == "__main__":
    md = "```json\n{\"msg\": \"Hello\"}\n```"
    print(extract_json_from_markdown(md))
    # 输出: {\"msg\": \"Hello\"}

def format_api_response(response: dict) -> dict:
    """
    Format API response for further processing
    """
    SQLBotLogUtil.debug(f"待解析的数据是：{response}")
    try:
        response = json.loads(response)
        data = response.get('BDP_QRY_OBJ_ASBG', [])
    except Exception as e:
        SQLBotLogUtil.error(f'取数结果解析报错：{str(e)}')
        data = ""

    if not data:
        data = ""

    return data
