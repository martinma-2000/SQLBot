# apps/indicator/parameter_generator.py

import requests
import json


def generate_input_parameters(sys_prompt, user_prompt):
    """
    Call LLM to generate input parameters for query
    """
    # 服务地址
    url = "http://IP:PORT/v1/chat/completions"

    # 构造消息体
    messages = [
        {"role": "system", "content": sys_prompt},
        {"role": "user", "content": user_prompt}
    ]

    # 请求参数
    payload = {
        "model": "Qwen3-32b",
        "messages": messages,
        "enable_thinking": False,
        "stream": False,
        "temperature": 0.0,
        "seed": 12345,
        "top_p": 1.0,
        "response_format": {"type": "json_object"}
    }

    # 发送POST请求
    try:
        response = requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=30
        )
        response.raise_for_status()

        # 解析响应结果
        result = response.json()
        sql_content = result["choices"][0]["message"]["content"]

        return sql_content.strip()

    except requests.exceptions.RequestException as e:
        raise Exception(f"调用LLM服务失败: {str(e)}")
    except (KeyError, json.JSONDecodeError) as e:
        raise Exception(f"解析LLM响应失败: {str(e)}")
    except Exception as e:
        raise Exception(f"生成SQL失败: {str(e)}")

def validate_parameters(params: dict, required_params: list) -> bool:
    """
    Validate if all required parameters are provided
    """
    pass
