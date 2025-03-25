
# 🎯 Prompt Engineering with OpenAI: Resume Coach & AML Data Analyst

This guide walks through two personas: **Resume Coach** and **Data Analyst for AML (Anti-Money Laundering)** investigations — complete with deployable code! 🚀

---

## 🧠 1. Setup OpenAI API

First, install the OpenAI library:

```bash
pip install openai
```

Set up your API key:

```python
import openai

openai.api_key = "your_openai_api_key"
```

---

# 🎯 Persona 1: Resume Coach

**Goal:** Help job seekers craft powerful, tailored resumes.

### ✨ Prompt Example: Rewrite a Resume Summary

```python
response = openai.Completion.create(
    engine="gpt-4",
    prompt="""
    Act as a professional resume coach. Rewrite the following resume summary to emphasize leadership and project management skills:
    'Experienced data analyst with a background in financial services. Skilled in SQL, Python, and visualization tools.'
    """,
    max_tokens=150,
    temperature=0.7
)

print(response.choices[0].text.strip())
```

🔹 **Output Example:**

> Proven Data Analyst with 5+ years in financial services, leading cross-functional teams to optimize data pipelines and deliver actionable insights. Expertise in SQL, Python, and advanced visualization, driving strategic decisions.

---

### 🚀 Persona 2: Data Analyst for AML Suspicious Trade Detection

**Goal:** Detect unusual patterns indicative of potential money laundering.

### 🔍 Prompt Example: Analyze Transaction Patterns

```python
transaction_data = '''
CustomerID,Amount,Country,Frequency
12345,12000,US,2
67890,95000,CN,7
34567,4000,US,35
67812,800000,RU,1
'''

response = openai.Completion.create(
    engine="gpt-4",
    prompt=f"""
    Act as a data analyst specializing in AML. Given this transaction data:
    {transaction_data}
    
    Identify suspicious patterns indicating potential money laundering. Consider amount, country, and frequency.
    Provide a summary of findings.
    """,
    max_tokens=200,
    temperature=0.5
)

print(response.choices[0].text.strip())
```

🔹 **Output Example:**

> The transaction from Customer 67812 stands out with an unusually high amount ($800,000) in a single transaction from Russia, coupled with a low frequency — a common red flag for placement stage money laundering. Customer 67890 also shows a high frequency of international transfers, which warrants further investigation.

---

## 📌 Deployment Tips

1. **Containerize the script** with Docker for easy deployment:
    ```dockerfile
    FROM python:3.9
    COPY requirements.txt /app/requirements.txt
    RUN pip install -r /app/requirements.txt
    COPY . /app
    CMD ["python", "/app/your_script.py"]
    ```

2. **Automate detection** by scheduling the AML script as a daily job on a server (e.g., using `cron`).

3. **Extend prompts** for more nuanced resume roles (e.g., technical vs. managerial resumes) or specialized AML behaviors (e.g., layering vs. integration stages).

---

✨ **Time to Build**

Now you’re equipped to engineer tailored prompts for both resume coaching and AML data analysis. Ready to take it further? 💪

