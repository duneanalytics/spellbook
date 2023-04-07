from langchain import PromptTemplate

request_prompt_template = """
Given the following extracted snippet of a long {syntax} SQL statement and a syntax error, 
return a correct SQL TRINO snippet with references ("SOURCES"). 

If you don't know the answer, just say that you don't know. Don't try to make up an answer.

SNIPPET: {snippet}
ERROR: {error}

Return the original, the correct SQL snippet, and sources.
DO NOT add any extra code to the correct snippet.
each YAML value should be all on single line

--- ALWAYS use the following YAML format for the output ---
--- 
original: [original snippet]
corrected: [corrected snippet]
sources: [sources]
---
"""

request_prompt_template = PromptTemplate(
    template=request_prompt_template, input_variables=["snippet", "error", "syntax"]
)


spell_prompt_template = """
Given the following extracted snippet of a long {syntax} SQL statement and a syntax error, 
return a correct SQL TRINO snippet with no added code using sources.

If you don't know the answer, return "No answer found". Don't try to make up an answer.

SNIPPET: {snippet}
ERROR: {error}

Return ONLY the correct SQL snippet
DO NOT add any extra code to the correct snippet
"""

spell_prompt_template = PromptTemplate(
    template=spell_prompt_template, input_variables=["snippet", "error", "syntax"]
)