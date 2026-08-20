from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from core.agent_state import AgentState

from core.nodes.classify_intent_node import classify_intent_node
from core.nodes.chitchat_node import chitchat_node
from core.nodes.retrieve_node import retrieve_node
from core.nodes.sql_gen_node import sql_gen_node
from core.nodes.execute_node import execute_node
from core.nodes.insight_node import insight_node

# [SỬA LỖI] Import thêm route_after_retrieve
from core.routers import route_after_classification, route_after_execute, route_after_retrieve

def build_agent_graph():
    workflow = StateGraph(AgentState)

    workflow.add_node("classify_intent_node", classify_intent_node)
    workflow.add_node("chitchat_node", chitchat_node)
    workflow.add_node("retrieve_node", retrieve_node)
    workflow.add_node("sql_gen_node", sql_gen_node)
    workflow.add_node("execute_node", execute_node)
    workflow.add_node("insight_node", insight_node)

    workflow.set_entry_point("classify_intent_node")

    workflow.add_conditional_edges(
        "classify_intent_node",
        route_after_classification,
        {
            "chitchat_node": "chitchat_node",
            "retrieve_node": "retrieve_node"
        }
    )

    workflow.add_edge("chitchat_node", END)

    # [SỬA LỖI] Bỏ add_edge cố định, thay bằng add_conditional_edges chặn lỗi
    workflow.add_conditional_edges(
        "retrieve_node",
        route_after_retrieve,
        {
            "sql_gen_node": "sql_gen_node", # Nếu success
            "insight_node": "insight_node"  # Nếu lỗi (đi thẳng ra output báo lỗi)
        }
    )
    
    workflow.add_edge("sql_gen_node", "execute_node")
    
    workflow.add_conditional_edges(
        "execute_node",
        route_after_execute,
        {
            "sql_gen_node": "sql_gen_node", 
            "insight_node": "insight_node"  
        }
    )
    
    workflow.add_edge("insight_node", END)

    memory = MemorySaver()
    app = workflow.compile(checkpointer=memory)
    
    return app

agent_app = build_agent_graph()