# Translation Docstore for DuneSQL

This spike is based loosely on the Langchain example for building a [question answering database on Notion](https://github.com/hwchase17/notion-qa). 

## Methodology
The inspiration for this spike is to turn the [Dune Migration docs](https://dune.com/docs/query/syntax-differences/#syntax-comparison) into a structured vector database that can be used to manage simple translation tasks. 

Vector databases using embedding similarity scores to return the top N most similar documents 
 to a query. (https://python.langchain.com/en/latest/modules/chains/index_examples/summarize.html?highlight=stuff#the-stuff-chain). If our docstore contains documents on how to do syntax translations, they will be inserted directly into the prompt. This allows us to provide a wide range of instructions but preserve precious token space and only insert them when needed. 

## Considerations
Max tokens constraint is a significant issue when considering LLM translation. For Spellbook, what we have landed on is only sending the lines surrounding and error and translating those. We think this is a better path than trying to translate entire queries that can be thousands of lines long. 

## Tests
Tests include a small set of query snippets that fail in Spellbook and the expected translation. 

## How to run this
Temporary instructions for running this spike.
1) Install virtual environment using the pipfile (`pipenv install` from llm-tests root) 
(stop yelling at me, I'll convert it to Docker-compose soon)
2) Either enter the virtual environment (`pipenv shell`) or configure your IDE to use the virtual environment.
3) Set your environment variables for OpenAI (`export OPENAI_API_KEY=123`).
4) Run the tests (`pytest` from llm-tests root).
5) Run the script (`python translation_docstore/main.py --snippet "Your sql to translate" --error "Your Error".) 
This script will also run with a default snippet and error


## TODO
Add many more rules to the doc store
Store the vector db in an accessible place
Add more unit tests
Connect to Alex's work on spells possibly via API?
