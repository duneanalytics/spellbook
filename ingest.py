"""This is the logic for ingesting SQL translation rules logs into LangChain."""
import argparse
from pathlib import Path
from langchain.text_splitter import CharacterTextSplitter
import faiss
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings
import pickle

parser = argparse.ArgumentParser(description="Which translation rules to ingest?")
parser.add_argument('syntax', nargs='?', type=str, default="spark", help="spark or postgres")
args = parser.parse_args()
syntax = args.syntax

# Ingest txt files with rules for making translations
ps = list(Path('rules/'+syntax + '/').glob("**/*.txt"))
data = []
sources = []
for p in ps:
    with open(p) as f:
        data.append(f.read())
    sources.append(p)

# Here we split the documents, as needed, into smaller chunks.
# We do this due to the context limits of the LLMs.
text_splitter = CharacterTextSplitter(chunk_size=1500, separator="\n")
docs = []
metadatas = []
for i, d in enumerate(data):
    print(f"Processing {i} of {len(data)}")
    splits = text_splitter.split_text(d)
    docs.extend(splits)
    metadatas.extend([{"source": sources[i]}] * len(splits))


# Here we create a vector store from the documents and save it to disk.
store = FAISS.from_texts(docs, OpenAIEmbeddings(), metadatas=metadatas)
faiss.write_index(store.index, f"docs_{syntax}.index")
store.index = None
with open(f"faiss_store_{syntax}.pkl", "wb") as f:
    pickle.dump(store, f)