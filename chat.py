import openai


def chat_request(model, instructions, input):
    question = f"""Can you translate the following section of a dbt model to valid Trino SQL? 
                   Please only return the correct dbt model snippet so that it can be inserted into the original. 
                   Always return the correct dbt model snippet between ``` characters.
                   dbt model: 
                   {input}"""
    initial_message = {"role": "system", "content": "You are a helpful assistant."}
    messages = [initial_message]
    for instruction in instructions:
        messages.append({"role": "assistant", "content": f"I know this translation {instruction}"})
    messages.append({"role": "user", "content": question})
    response = openai.ChatCompletion.create(
        model=model,
        messages=messages,
        temperature=0
    )
    return response