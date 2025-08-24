# 🚀 AI Model Setup Guide  - TBModified

This guide helps you quickly set up **OpenAI**, **Langchain**, **Claude**, and **LLaMA** in Python. Let’s dive in!  🎯

---

## 🔹 1. OpenAI Setup

First, install the OpenAI library:

```bash
pip install openai
``` 

Basic usage example:

```python
import openai

openai.api_key = "your_openai_api_key"

response = openai.Completion.create(
    engine="gpt-4",
    prompt="Explain quantum physics in simple terms.",
    max_tokens=200
)

print(response.choices[0].text.strip())
```

---

## 🔧 2. Langchain Setup (with OpenAI)

Install Langchain and OpenAI:

```bash
pip install langchain openai
```

Quick example:

```python
from langchain.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.chains import LLMChain

llm = ChatOpenAI(temperature=0.7, model="gpt-4", openai_api_key="your_openai_api_key")

prompt = ChatPromptTemplate.from_template("Explain {topic} in a funny way.")
chain = LLMChain(llm=llm, prompt=prompt)

response = chain.run(topic="black holes")
print(response)
```

---

## 🤖 3. Claude Setup (Anthropic API)

Install the `anthropic` package:

```bash
pip install anthropic
```

Basic usage with Claude:

```python
import anthropic

client = anthropic.Anthropic(api_key="your_anthropic_api_key")

response = client.completions.create(
    model="claude-2",
    prompt="Explain how neural networks work in layman's terms.",
    max_tokens=200
)

print(response.completion)
```
---

## 🦙 4. LLaMA Setup (Meta’s model)

For LLaMA, use Hugging Face’s `transformers`:

```bash
pip install transformers torch
```

Quick example:

```python
from transformers import AutoTokenizer, AutoModelForCausalLM

model_name = "meta-llama/Llama-2-7b-chat-hf"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(model_name)

input_text = "Explain the importance of data privacy."
inputs = tokenizer(input_text, return_tensors="pt")
outputs = model.generate(**inputs, max_length=200)

print(tokenizer.decode(outputs[0], skip_special_tokens=True))
```

---

### 🎉 Ready to Build?
Happy coding! 💻🔥

