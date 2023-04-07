"""Ask a question to the vector database."""
import argparse
import pickle

import faiss
from langchain import OpenAI
from langchain.chains import RetrievalQAWithSourcesChain

from translation_docstore.prompts import spell_prompt_template

parser = argparse.ArgumentParser(description="SQL translation tool with rules vector db")
parser.add_argument('model_path', type=str, nargs='?', help="model_path", default=default_snippet)
args = parser.parse_args()

model_path = args.model_path
syntax = 'spark'

def load_chain():
    # Load the LangChain.
    index = faiss.read_index(f"docs_{syntax}.index")

    with open(f"faiss_store_{syntax}.pkl", "rb") as f:
        store = pickle.load(f)

    store.index = index
    chain = RetrievalQAWithSourcesChain.from_chain_type(OpenAI(model_name='gpt-3.5-turbo', temperature=0), chain_type="stuff",
                                                        retriever=store.as_retriever())
    return chain

def get_sql_snippet_and_error(model_path):
    with open(model_path, 'r') as f:
        snippet = f.readline()
        error = f.readline()
    return snippet, error

prompt = spell_prompt_template.format(snippet=snippet, error=error, syntax=syntax)
result = chain({"question": prompt}, return_only_outputs=True)
print(f"Answer: {result['answer']}")
print(f"Sources: {result['sources']}")
