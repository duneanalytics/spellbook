"""Ask a question to the vector database."""
import argparse
import os
import pickle
import shutil
import subprocess

import faiss
from langchain import OpenAI
from langchain.chains import RetrievalQAWithSourcesChain

from explain_cls import Explain_n_Executer
from line_mapper import LineMapper
from prompts import spell_prompt_template

parser = argparse.ArgumentParser(description="SQL translation tool with rules vector db")
parser.add_argument('model_path', type=str, nargs='?', help="Path to model", default="models/aave/ethereum/aave_ethereum_votes.sql")
args = parser.parse_args()

model_path = args.model_path
syntax = 'spark'

def load_chain():
    # Load the LangChain.
    index = faiss.read_index(f"docs_{syntax}.index")

    with open(f"faiss_store_{syntax}.pkl", "rb") as f:
        store = pickle.load(f)

    store.index = index
    chain = RetrievalQAWithSourcesChain.from_chain_type(OpenAI(temperature=0), chain_type="stuff",
                                                        retriever=store.as_retriever())
    return chain


chain = load_chain()

def get_query_artifacts(model_path):
    explainer = Explain_n_Executer(model_path=f"spellbook/target/compiled/{model_path}")
    explainer.explain()

    # If NONE exit
    if not explainer.explanation:
        print("No Syntax Error")
        exit()

    print(f"Error: {explainer.explanation}")

    line_no = int(explainer.explanation.split("line ")[1].split(":")[0]) - 1 # Line no. one off because we added the EXPLAIN statement.
    sql_lines = explainer.sql.splitlines()
    snippet = sql_lines[line_no]
    print(f"Snippet: {snippet}")
    return explainer, line_no, snippet

explainer, line_no, snippet = get_query_artifacts(model_path)

prompt = spell_prompt_template.format(snippet=snippet, error=explainer.explanation, syntax=snippet)
result = chain({"question": prompt}, return_only_outputs=True)

# If chatgpt returns no answer, exit
if result['answer'] == "No answer found":
    print("No answer found")
    exit()

print(f"Corrected SQL: {result['answer']}")

line_mapper = LineMapper(spell_path=model_path,
                         compiled_path=f"spellbook/target/compiled/{model_path}",
                         start=line_no,
                         end=line_no+1)
spell_line_no = line_mapper.main()

# Get SQL from Spell
with open(model_path, 'r') as f:
    sql = f.readlines()

# Replace spell line with LLM correction
sql[spell_line_no] = result['answer']

# Copy original model
shutil.copy(model_path, f"{model_path}.original")

# Join SQL line to SQL string and write to file
print("overwriting model with corrected SQL")
output = "".join(sql)
with open(model_path, "w") as f:
    f.write(output)

os.chdir("spellbook")
print("Compiling model")
subprocess.run("dbt compile", shell=True, stdout=subprocess.PIPE)
os.chdir("..")

new_explainer, new_line_no, new_snippet = get_query_artifacts(model_path)

if not new_line_no != line_no:
    print("Revert changes, no change in error location")
    shutil.copy(f"{model_path}.original", model_path)