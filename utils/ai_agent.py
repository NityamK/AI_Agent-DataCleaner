import os
import pandas as pd
from dotenv import load_dotenv
from langchain_groq import ChatGroq
from langgraph.graph import StateGraph, END
from pydantic import BaseModel

# Load API key from environment
load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")

if not groq_api_key:
    raise ValueError("❌ GROQ_API_KEY is missing. Set it in .env or as an environment variable.")

# Define AI Model using DeepSeek LLM 
llm = ChatGroq(groq_api_key=groq_api_key, model_name="deepseek-r1-distill-llama-70b")

class CleaningState(BaseModel):
    """State schema defining input and output for the LangGraph agent."""
    input_text: str
    structured_response: str = ""

class AIAgent:
    def __init__(self):
        self.graph = self.create_graph()

    def create_graph(self):
        """Creates and returns a LangGraph agent graph with state management."""
        graph = StateGraph(CleaningState)

        def agent_logic(state: CleaningState) -> CleaningState:
            """Processes input using DeepSeek via Groq API."""
            response = llm.invoke(state.input_text)

            if isinstance(response, dict) and "content" in response:
                response_text = response["content"]  # Extract text from dictionary response
            elif hasattr(response, "content"):  
                response_text = response.content  # Extract content from AIMessage
            else:
                raise ValueError(f"Unexpected response type from LLM: {type(response)}")

            return CleaningState(input_text=state.input_text, structured_response=response_text)


        graph.add_node("cleaning_agent", agent_logic)
        graph.add_edge("cleaning_agent", END)
        graph.set_entry_point("cleaning_agent")
        return graph.compile()

    def process_data(self, df, batch_size=20):
        """Processes data in batches using Groq API."""
        cleaned_responses = []

        for i in range(0, len(df), batch_size):
            df_batch = df.iloc[i:i + batch_size]

            prompt = f"""
            You are an AI Data Cleaning Agent. Analyze the dataset:

            {df_batch.to_string()}

            - Identify missing values and impute them (mean, mode, median)
            - Remove duplicate rows
            - Format text values correctly (e.g., title case for names)
            - Normalize numeric values (if applicable)
            - Return the cleaned data in a structured format (table format or JSON).
            """

            state = CleaningState(input_text=prompt, structured_response="")
            response = self.graph.invoke(state)

            if isinstance(response, dict):
                response = CleaningState(**response)

            cleaned_responses.append(response.structured_response)

        return "\n".join(cleaned_responses)
