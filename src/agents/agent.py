"""
分录对账智能体
基于LangGraph实现的财务对账分析Agent
"""
from typing import Annotated, Sequence
from langchain.agents import create_agent
from langchain_openai import ChatOpenAI
from langgraph.graph import MessagesState
from langgraph.graph.message import add_messages
from langchain_core.messages import AnyMessage, BaseMessage
from storage.memory.memory_saver import get_memory_saver
from config import llm_settings
from agents.system_prompt import SYSTEM_PROMPT

from tools.reconciliation_tool import load_je_data, load_tb_data, run_reconciliation

# 默认保留最近 20 轮对话 (40 条消息)
MAX_MESSAGES = 40


def _windowed_messages(old: Sequence[BaseMessage], new: Sequence[BaseMessage]) -> Sequence[BaseMessage]:
    """滑动窗口: 只保留最近 MAX_MESSAGES 条消息"""
    result: Sequence[BaseMessage] = add_messages(old, new)  # type: ignore
    return list(result)[-MAX_MESSAGES:]


class AgentState(MessagesState):
    messages: Annotated[list[AnyMessage], _windowed_messages]  # type: ignore


def build_agent(ctx=None):
    """构建分录对账Agent"""
    settings = llm_settings()

    llm = ChatOpenAI(
        model=settings.model,
        api_key=settings.api_key,
        base_url=settings.base_url,
        temperature=settings.temperature,
        streaming=True,
        timeout=settings.timeout_seconds,
        max_tokens=settings.max_tokens,
        extra_body=settings.extra_body,
    )

    tools = [load_je_data, load_tb_data, run_reconciliation]

    return create_agent(
        model=llm,
        system_prompt=SYSTEM_PROMPT,
        tools=tools,
        checkpointer=get_memory_saver(),
        state_schema=AgentState,
    )
