from pydantic import BaseModel, Field
from typing import Optional


# 定义 Agent A 必须返回的参数结构
class FinancialDataParams(BaseModel):
    """
    用户请求中提取出的公司和年份信息。
    """
    company_name: str = Field(
        ..., description="从用户输入中提取的准确公司名称，例如“平安银行”或“腾讯控股”。"
    )
    start_year: int = Field(
        ..., description="从用户输入中提取的财报收集开始年份（YYYY）。"
    )
    end_year: Optional[int] = Field(
        None, description="从用户输入中提取的财报收集结束年份（YYYY）。如果未明确提供，则默认为开始年份。"
    )

    # 🌟 新增字段：用于确认
    needs_confirmation: bool = Field(
        True, description="如果模型成功提取了所有参数，将此字段设置为 True，表示需要用户确认。"
    )